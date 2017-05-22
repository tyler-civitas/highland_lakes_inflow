"""
BORROWED FROM GITHUB USER NATHANHILBERT
This was used as a 'comparison' template for the webscraping work

download data for LCRA gauges and add them to SWIS database
"""
import argparse
from decimal import Decimal
import datetime
from time import sleep


from BeautifulSoup import BeautifulSoup
import numpy as np
import requests

from swis.database import db_session
from swis import models, util
from swis.models.log import log_message

import data_util as du

#: Organization Abbreviated Name
org_abbr = 'LCRA'
ORG_DIR = 'lcra/'
HISTORICAL_DAILY_FILE = util.data_path('lcra/LakeElevation_Daily_HighlandLakes.csv')

# Email from Bob Huber <Bob.Huber@LCRA.ORG> dated 07/09/2012
# Central time and changes from standard to daylight according to the rules in effect for that year.
# Starting in 2007 Daylight Savings Time in the United States
# begins on the Second Sunday of March    at 2:00 a.m. local time and
# ends   on the First  Sunday of November at 2:00 a.m. local time
# The jump in times occurs in spring, because time shifts forward on hour.
# (An example would be March 13, 2011). There is no jump in time in the fall because the data may be
# ignored for one hour. We use the data in real-time for operations decisions, but it may not be saved.


def import_gauge_data(gauges, historical=False):

    source = util.query_or_new(db_session, models.Source, dict(
        name='LCRA Hydromet application', organization=gauges[0].organization,
        notes='url: http://hydromet.lcra.org/chronhist.aspx'))

    swis_parameter_elev = du.get_swis_parameter('water_surface_elevation', None, None)
    values_model = models.GaugeValue

    for gauge in gauges:
        conversion_func = lambda x: x

        elev_data = webscrape_lcra_hydromet(gauge, historical=historical)

        du.swis_upsert_data(gauge, source, values_model, conversion_func, swis_parameter_elev, elev_data)
        if historical:
            elev_data = import_historical_from_csv(gauge)
            du.swis_upsert_data(gauge, source, values_model, conversion_func, swis_parameter_elev, elev_data)


def import_gauges():
    """
    Create new organization and then retrieve a list of gauges and gauge metadata
    and insert this information into the swis database.
    LCRA Gauge Metadata retrieved rom associated USGS gauges since they are the same.
    1995: Buchanan Dam (Lake Buchanan)
    1999: Inks Dam (Inks Lake)
    3963: Mansfield Dam (Lake Travis)
    2999: Starcke Dam (Lake Marble Falls) // not present in database currently or in USGS
    3999: Tom Miller Dam (Lake Austin)
    2958: Wirtz Dam (Lake LBJ)
    associated USGS gauges:
    08148000     LCRA Lk Buchanan nr Burnet, TX
    08148100     LCRA Inks Lk nr Kingsland, TX
    08152500     LCRA Lk LBJ nr Marble Falls, TX
    08154500     LCRA Lk Travis nr Austin, TX
    08154900     LCRA Lk Austin at Austin, TX
    """
    gauge_list = [('1995', 'Buchanan Dam', '08148000', -0.01, 0.25),
                 ('1999', 'Inks Dam', '08148100', 0.05, 0.36),
                 ('3963', 'Mansfield Dam', '08154500', -0.40, 0.20),
                 #('2999', 'Starcke Dam', '08152500'), #not currently in db or in USGS service
                 ('3999', 'Tom Miller Dam', '08154900', 0.04, 0.35),
                 ('2958', 'Wirtz Dam', '08152500', -0.32, 0.36)]

    print 'updating %s gauge metadata.' % org_abbr

    #: Setup new organization
    organization = util.query_or_new(db_session, models.Organization, dict(
            full_name='Lower Colorado River Authority', abbreviation=org_abbr))

    usgs_org = db_session.query(models.Organization).filter(models.Organization.abbreviation == 'USGS').one()

    #: Add each Gauge ...
    for code, name, usgs_code, offset_from_ngvd29, ngvd29_to_navd88 in gauge_list:
        print 'importing %s:%s metadata' % (code, name)
        usgs_gauge = db_session.query(models.Gauge).filter(models.Gauge.organization == usgs_org).filter_by(code=usgs_code).one()
        latitude = usgs_gauge.latitude
        longitude = usgs_gauge.longitude
        gauge = du.get_or_add_swis_gauge(code, name, latitude, longitude, organization, ngvd29_to_navd88)

        #add vertical datum
        #todo get datum from USGS gauge
        util.update_or_new(db_session, models.GaugeVerticalDatum, dict(gauge_id=gauge.id),
                dict(datum='NGVD29', offset=offset_from_ngvd29, gauge=gauge))

   # Add starcke dam
    # As per email from LCRA, the lat/long for the Starcke Dam gauge is 30deg33min21secN, -98deg15min26secW.
    longitude = -1*(98. + 15./60. + 26./3600.)
    latitude = 30. + 33./60. + 21./3600.
    gauge = du.get_or_add_swis_gauge('2999', 'Starcke Dam', latitude, longitude, organization, ngvd29_to_navd88=0.19)

    #add vertical datum
    #todo get datum from LCRA
    util.query_or_new(db_session, models.GaugeVerticalDatum, dict(
        datum='NGVD29', offset=-0.50, gauge=gauge))


def import_historical_from_csv(gauge):
    """
    Read in historical beginning of day data provided by LCRA
    From: Bob Huber, P.E. , (512)473-3200 ext 7903, Bob.Huber@lcra.org
    received on 3/13/2012
    """

    def dt(x):
        """ make date with appropriate century """
        m, d, y = [int(n) for n in x.split('/')]
        if y < 30:
            y += 2000
        else:
            y += 1900
        return du.cst2utc_with_dst(datetime.datetime(y,  m,  d, 0, 0, 0))

    print 'Importing historical beginning of day data for LCRA gauge:%s from file %s' % (gauge.code, HISTORICAL_DAILY_FILE)
    #gauge_list in column order
    gauge_list = ['1995',  # 'Buchanan Dam'),
                  '1999',  # 'Inks Dam'),
                  '2958',  # 'Wirtz Dam')]
                  '2999',  # 'Starcke Dam'),
                  '3963',  # 'Mansfield Dam'),
                  '3999',  # 'Tom Miller Dam'),
                  ]
    data = (np.genfromtxt(HISTORICAL_DAILY_FILE, delimiter=',', skip_header=2, missing_values=True,
                          dtype=None,  converters={0: dt},  names=['date'] + gauge_list,  usecols=(0, 1, 2, 3, 4, 5, 6)))

    elevs = data[gauge.code]
    valid_dates = np.isfinite(elevs)
    dates = data['date'][valid_dates]
    elevs = elevs[valid_dates]

    elev_data = [[date, str(elev)] for date, elev in zip(dates, elevs)]
    return elev_data


def update():
    """
    Get recent gauge data. For use with celery.
    """
    gauges = du.organization_gauges(org_abbr)
    with log_message('importing recent %s gauge data' % org_abbr):
        import_gauge_data(gauges)
    du.update_caches_for_gauges(gauges)


def webscrape_lcra_hydromet(gauge, days=7, historical=False):
    """
    scrapes lake level data for LCRA lake guages from lcra hydromet website
    """
    url = 'http://hydromet.lcra.org/chronhist.aspx'

    # make an initial request
    initial_request = requests.get(url)

    # make the request to update the dropdown list
    list_request_headers = {
        '__EVENTTARGET': 'DropDownList1',
        'DropDownList1': gauge.code,
        'DropDownList2': 'STAGE',
    }
    list_request = _make_next_request(url, initial_request, list_request_headers)

    # make a final request for data
    edate = datetime.datetime.now() + datetime.timedelta(days=1)
    sdate = edate - datetime.timedelta(days=days)
    data_request_headers = {
        'Date1': sdate.strftime('%-m/%-d/%Y'),
        'Date2': edate.strftime('%-m/%-d/%Y'),
        'DropDownList1': gauge.code,
        'DropDownList2': 'HEAD',
    }

    print 'scraping last %s days of data for LCRA gauge %s:%s' % (days, gauge.code, gauge.name)
    data_request = _make_next_request(url, list_request, data_request_headers)
    elev_data = _extract_data(data_request)

    if historical:
        tags = BeautifulSoup(data_request.content).findAll('font', {'color': '#333333'})

        # cycle back 179 days at a time
        while len(tags) > 0:
            edate = sdate - datetime.timedelta(days=1)
            sdate = edate - datetime.timedelta(days=179)
            data_request_headers = {
                'Date1': sdate.strftime('%-m/%-d/%Y'),
                'Date2': edate.strftime('%-m/%-d/%Y'),
                'DropDownList1': gauge.code,
                'DropDownList2': 'HEAD',
                }

            print 'scraping %s to %s of data for LCRA gauge %s:%s' % (data_request_headers['Date1'], data_request_headers['Date2'], gauge.code, gauge.name)
            data_request = _make_next_request(url, list_request, data_request_headers)
            elev_data.extend(_extract_data(data_request))
            tags = BeautifulSoup(data_request.content).findAll('font', {'color': '#333333'})

    return du.uniq_ts(elev_data)


def _extract_data(request):
    tags = BeautifulSoup(request.content).findAll('font', {'color': '#333333'})
    elev_data = [
        _value_pair_from_tags(date_tag, value_tag)
        for date_tag, value_tag in zip(tags[::2], tags[1::2])]
    return elev_data


def _extract_headers_for_next_request(request):
    payload = dict()
    for tag in BeautifulSoup(request.content).findAll('input'):
        tag_dict = dict(tag.attrs)
        payload[tag_dict['name']] = tag_dict['value']
    return payload


def _make_next_request(url, previous_request, data):
    data_headers = _extract_headers_for_next_request(previous_request)
    data_headers.update(data)
    return requests.post(url, cookies=previous_request.cookies, data=data_headers)


def _value_pair_from_tags(date_tag, value_tag):
    timestamp_utc = du.cst2utc_with_dst(datetime.datetime.strptime(date_tag.text, '%b %d %Y %I:%M%p'))
    return [timestamp_utc, value_tag.text]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='import %s gauge data' % org_abbr)
    parser.add_argument('--historical', action='store_true', default=False)
    parser.add_argument('--init', action='store_true', default=False)
    historical = parser.parse_args().historical
    init = parser.parse_args().init

    #initialize gauge information
    if init:
        with log_message('updating %s gauge metadata' % org_abbr):
            import_gauges()

    org = db_session.query(models.Organization).filter(models.Organization.abbreviation == org_abbr).one()
    gauges = db_session.query(models.Gauge).filter(models.Gauge.organization == org).all()

    if historical:
        with log_message('importing historical %s gauge data' % org_abbr):
            import_gauge_data(gauges, historical=True)
    else:
        with log_message('importing current %s gauge data' % org_abbr):
            import_gauge_data(gauges)


```
