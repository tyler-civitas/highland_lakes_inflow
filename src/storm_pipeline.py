"""
Python scripts for storm event discovery and aggregation
"""
from sql_class import ManipulateDatabase
import numpy as np
import pandas as pd
import datetime
from collections import defaultdict


def aggregate_rain_data_by_storm(stormlist):
    """Aggregates sensor data for the given sensor
    Sums all value columns. Grouped by gauge, sensor.

    The data point inherits a 'start time', 'end time' column
    from the stormlist, and the values data comes from
    dataframe.


    Returns X, y as dataframes. y has multiple lakes to choose
    Result |start time| end time| gauge_1| gauge_2| gauge 3... etc
    Result |start time| end time| lake_1 | lake_2|... etc

    """
    ### THIS ENTIRE FUNCTION CAN BE REWRITTEN TO UTILIZED
    # TWO CROSSTAB (PIVOT) TABLES IN POSTGRESQL WHICH RETURN
    # SUM OF VALUES GROUPED BY COLLECTION TIME ACROSS ALL
    # GAUGES



    md = ManipulateDatabase()
    md.load_dbinfo_server()
    md.connect()
    df = pd.DataFrame()

    mainlakelist = [
        str(3999), str(3963), str(1995), str(2958), str(2999), str(1999)
                   ]

    lakes = [
        'start_time', 'end_time', str(3999) + "_max",
        str(3963) + "_max", str(1995) + "_max", str(2958) + "_max",
        str(2999) + "_max", str(1999) + "_max", str(3999) + "_min",
        str(3963) + "_min", str(1995) + "_min", str(2958) + "_min",
        str(2999) + "_min", str(1999) + "_min"
            ]

    mainrainlist = [
        str(4594), str(3991), str(3948),
        str(3448), str(3237), str(3015), str(2634), str(2348),
        str(2140), str(2248), str(1921), str(1405), str(1090),
        str(1307), str(1197)
                   ]

    rain = [
        'start_time', 'end_time', str(4594), str(3991), str(3948),
        str(3448), str(3237), str(3015), str(2634), str(2348),
        str(2140), str(2248), str(1921), str(1405), str(1090),
        str(1307), str(1197)
           ]

    startlist = []
    endlist = []
    gaugedict = defaultdict(list)
    maxdict = defaultdict(list)
    mindict = defaultdict(list)

    i = 0
    for storm_start, storm_end in stormlist:
        i += 1
        if i % 50 == 0:
            print "starting loop {}".format(i)

        raindata = md.get_storm_rainfall(storm_start, storm_end)
        startlist.append(storm_start)
        endlist.append(storm_end)

        raindatadict = {gauge: value for gauge, value in raindata}
        for gauge in mainrainlist:
            gaugedict[gauge].append(raindatadict.get(gauge, None))

        lakedata = md.get_max_min_lakes(storm_start, storm_end)

        lakedatamindict = {gauge: mini for gauge, mini, maxi in lakedata}
        lakedatamaxdict = {gauge: maxi for gauge, mini, maxi in lakedata}
        for gauge in mainlakelist:
            maxdict[gauge].append(lakedatamaxdict.get(gauge, None))
            mindict[gauge].append(lakedatamindict.get(gauge, None))


    df['start_time'] = startlist
    df['end_time'] = endlist
    for gauge in gaugedict:
        df[gauge] = gaugedict[gauge]
    for gauge in maxdict:
        label = str(gauge) + "_max"
        df[label] = maxdict[gauge]
    for gauge in mindict:
        label = str(gauge) + "_min"
        df[label] = mindict[gauge]

    return df.loc[:, rain], df.loc[:, lakes]


def create_moving_sum(df, leading_hours, trailing_hours):
    """Leading Hours - Hours after event
    trailing hours - Hours before event
    """

    # Clean out zero rain values
    rain_greater_than_zero = df.iloc[:, 1] >= 0
    rainclean = df.iloc[:, 1][rain_greater_than_zero]
    rainclean.shape
    df['Rain (inches)'] = rainclean

    # Set collection time to index
    df.sort_values(["collection_time"], inplace=True)
    df.set_index("collection_time", drop=True, inplace=True)

    #Create List
    lead_delta = datetime.timedelta(hours=leading_hours)
    trail_delta = datetime.timedelta(hours=trailing_hours)

    moving_sum_list = []
    i = 0
    for row in df.iterrows():
        i += 1
    #     print row[1][0] #time itself
    #     print row[1][0] - trail_delta #begin of window
    #     print row[1][0] + lead_delta # end of window
    #     print row[1][1] # value

        start = row[0] - trail_delta
        end = row[0] + lead_delta
        # IF ROWS ARE EVENLY SPACED TIME-WISE, TRY USING ROWS INSTEAD OF TIME
        # window = df[(df['collection_time'] > start) & (df['collection_time'] < end)]
        window = df[start:end]
        moving_sum_list.append(window["Rain (inches)"].sum())
        if i % 100000 == 0:
            print "appended row {}".format(i)

    return moving_sum_list


def define_storm_events(df, threshold=2):
    """
    INPUT - DataFrame [collection_time | Rain | moving_rain_sum]
    OUTPUT - [(start storm, end storm), ....] as datetimes
    Threshold is only based on maximum inches from any gauge...
    not a good indicator of actual inches fallen - but helps
    define when storms are anywhere in the system
    """
    df.reset_index(drop=False, inplace=True)
    df.sort_values(["collection_time"], inplace=True)

    dfmrs = df['moving_rain_sum']

    stormlist = []

    for i in xrange(1, df.shape[0]):
        if ((dfmrs[i] > threshold) and
           (dfmrs[i - 1] < threshold)):

            storm_start = df['collection_time'][i]

        elif ((dfmrs[i] < threshold) and
            (dfmrs[i - 1] > threshold)):

            storm_end = df['collection_time'][i]
            stormlist.append((storm_start, storm_end))

        if i % 100000 == 0:
            print "Now at {}".format(i)

    return stormlist


def column_ids_to_names(df):

    gauge_dict = {
    "3999_min": 'Tom Miller Dam_min',
    "3999_max": 'Tom Miller Dam_max',
    "4594": 'Driftwood 4 SSE',
    "3991": 'Jollyville 2 SW',
    "3963_min": 'Mansfield Dam_min',
    "3963_max": 'Mansfield Dam_max',
    "3948": 'Lakeway 2 E',
    "3448": 'Blanco 5 NNE',
    "3237": 'Harper 4 SSW',
    "3015": 'Burnet 1 WSW',
    "2634": 'Cherokee 4 SSE',
    "2348": 'Menard 12 SSE',
    "2140": 'Sonora 14 SE',
    "2248": 'Rocksprings 12 NE',
    "1995_min": 'Buchanan Dam_min',
    "1995_max": 'Buchanan Dam_max',
    "1921": 'Lometa 2 WNW',
    "1405": 'Eldorado 2 E',
    "1090": 'Millersview 7 WSW',
    "1307": 'Clyde 6 S',
    "1197": 'Rochelle 5 NNW',
    "2958_min": 'Wirtz Dam_min',
    "2958_max": 'Wirtz Dam_max',
    "2999_min": 'Starcke Dam_min',
    "2999_max": 'Starcke Dam_max',
    "1999_min": 'Inks Dam_min',
    "1999_max": 'Inks Dam_max'
             }

    return df.rename(columns=gauge_dict).copy()


def convert_lake_levels_to_volumes(df):
    """One-off script to convert lake levels to volumes"""

    df = df.round(1)
    for column in df:
        if ((column == "Mansfield Dam_min") or
           (column == "Mansfield Dam_max")):
            table = pd.read_csv("tables/Travis.txt")
            table.set_index(['ft-MSL'], inplace=True)
            elevdict = table.to_dict()['acre-feet']
            df[column] = df[column].map(elevdict)

        elif ((column == "Buchanan Dam_min") or
             (column == "Buchanan Dam_max")):
            table = pd.read_csv("tables/Buchanan.txt")
            table.set_index(['ft-MSL'], inplace=True)
            elevdict = table.to_dict()['acre-feet']
            df[column] = df[column].map(elevdict)


    remove =['Wirtz Dam_min', 'Wirtz Dam_max', 'Starcke Dam_min',
             'Starcke Dam_max', 'Inks Dam_min', 'Inks Dam_max',
             'Tom Miller Dam_max', 'Tom Miller Dam_min']

    for item in remove:
        del df[item]

    return df

if __name__ == "__main__":
    import datetime
    from src.sql_class import ManipulateDatabase
    import pandas as pd

    md = ManipulateDatabase()
    md.load_dbinfo_server()
    md.connect()
    data = md.query_max_precip()
    print 'max precip done'
    df = pd.DataFrame(data, columns=["collection_time", "Rain (inches)"])## LIMIT
    print 'df built'
    df['moving_rain_sum'] = create_moving_sum(df, leading_hours=10, trailing_hours=24)

    df.to_pickle('../pickled_files/movingsumcomplete.pkl')



    # Aggregate data including SQL queries (Can have improvement using
    # pivottables

    df = pd.read_pickle('pickled_files/movingsumcomplete.pkl')

    thresholds = [0.25, 0.5, 1.5, 2.5, 1]
    for current_thres in thresholds:
        df = pd.read_pickle('pickled_files/movingsumcomplete.pkl')
        storms = define_storm_events(df, threshold=current_thres)
        X, y_all = aggregate_rain_data_by_storm(storms)

        xlabel = "pickled_files/X_10_24_thres" + str(current_thres) + ".pkl"
        ylabel = "pickled_files/y_all_10_24_thres" + str(current_thres) + ".pkl"

        X.to_pickle(xlabel)
        y_all.to_pickle(ylabel)


        # CONVERT NAMED AND LAKE VOLUMES

    pickledX = ["X_10_24_thres0.25.pkl", "X_10_24_thres0.5.pkl",
               "X_10_24_thres1.pkl", "X_10_24_thres1.5.pkl", "X_10_24_thres2.5.pkl"]

    pickledy = ["y_all_10_24_thres0.25.pkl", "y_all_10_24_thres0.5.pkl",
               "y_all_10_24_thres1.pkl", "y_all_10_24_thres1.5.pkl", "y_all_10_24_thres2.5.pkl"]

    pickledX.extend(pickledy)
    for filename in pickledX:
        direc = "pickled_files/" + filename
        df = pd.read_pickle(direc)
        df = column_ids_to_names(df)
        if filename[0] == 'y':
            df = convert_lake_levels_to_volumes(df)

        direc = "pickled_files/final_" + filename
        df.to_pickle(direc)
