"""
Contains class ManipulateDatabase

ManipulateDatabase can be used to connect to SQL server and insert
rows.

__main__ block is used for testing
"""

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import os


class ManipulateDatabase(object):
    """
    An object used to manipulate the postgres database
    """

    def __init__(self,
                 user='tyler',
                 password='',
                 db='highland_lakes',
                 host='localhost'):
        self.user = user
        self.password = password
        self.db = db
        self.host = host

        self.conn = None


    def connect(self):
        """Creates database connection"""
        self.conn = psycopg2.connect(
            dbname=self.db,
            user=self.user,
            host=self.host,
            password=self.password
           )

    def load_dbinfo_server(self):
        """Loads credentials from ~/.aws/hldb.txt"""
        home = os.getenv("HOME")
        path = home + "/.aws/hldb.txt"
        with open(path, "r") as f:
            self.user = f.readline().strip()
            self.password = f.readline().strip()
            self.db = f.readline().strip()
            self.host = f.readline().strip()[:-5]

    def create_table(self):
        """Create the table from scratch"""

        if self._check_table_existence():
            raise RuntimeWarning("Table exists! Drop it first!")
        cur = self.conn.cursor()
        cur.execute('''CREATE TABLE hydromet
                              (observation_id SERIAL PRIMARY KEY,
                               collection_time TIMESTAMP,
                               gauge TEXT,
                               sensor TEXT,
                               value REAL
                              );
                    '''
                   )
        self.conn.commit()
        cur.close()


    def insert_gauge_readings(self, obs):
        """Insert a single reading or multiple readings
         from an observation tuple

        Format: (timestamp, gauge, sensor, value) (As list for multi)

        NOTE: Some entries in hydromet have multiple values, such as
        'count', 'rainfall'. These should be stored as seperate
        entries. Sensor is the table header not the page element.

        Table name is hard-coded for parameterization
        """
        cur = self.conn.cursor()
        q = '''INSERT INTO hydromet
               (collection_time, gauge, sensor, value)
               VALUES %s'''

        if isinstance(obs, tuple):
            cur.execute(q, (obs,))
        else:
            tm = "(%s, %s, %s, %s)"
            execute_values(cur, q, obs, template=tm, page_size=999)

        self.conn.commit()
        cur.close()


    def _check_table_existence(self):
        """Checks if the database exists in PostgresSQl"""
        cur = self.conn.cursor()
        cur.execute('''SELECT *
                       FROM information_schema.tables
                       WHERE table_name=%s''',
                   ('hydromet',)
                   )
        cur.close()
        return bool(cur.rowcount)


    def query_max_precip(self):

        cur = self.conn.cursor()
        cur.execute('''SELECT collection_time, MAX(value)
                       FROM hydromet
                       WHERE sensor=%s
                       GROUP BY collection_time''',
                   ('Rain (inches)',)
                   )
        data = cur.fetchall()

        cur.close()
        return data

    def get_storm_rainfall(self, start_time, end_time):

        cur = self.conn.cursor()
        q = """
        SELECT gauge, SUM(value)
        FROM hydromet
        WHERE (collection_time BETWEEN %s AND %s) AND (sensor = 'Rain (inches)')
        GROUP BY gauge;
        """
        cur.execute(q, (start_time, end_time))

        data = cur.fetchall()
        cur.close()
        return data

    def get_max_min_lakes(self, start_time, end_time):

        cur = self.conn.cursor()
        q = """
        SELECT DISTINCT gauge, MIN(value), MAX(value)
        FROM hydromet
        WHERE (collection_time BETWEEN %s AND %s) AND
              (sensor = 'Lake Level (ft above MSL)')
        GROUP BY gauge
        """
        cur.execute(q, (start_time, end_time))

        data = cur.fetchall()
        cur.close()
        return data


#  observation_id |   collection_time   | gauge | sensor | value

# POOR MANS UNIT TESTING ENVIRONMENT
#~/anaconda2/lib/python2.7/site-packages/phantomjs
#~/anaconda2/lib/python2.7/site-packages/selenium/webdriver/phantomjs

#~/phantomjs-2.1.1-linux-x86_64/bin


if __name__ == "__main__":
    pass

# TEST Class Instantiation - OK
    md = ManipulateDatabase()
    md.load_dbinfo_server()
    md.connect()


# TEST Table Creation, TEST _check_table_existence() - OK
    # md.create_table()

# TEST insert_gauge_readings single and multi - OK
# TEST multiple commits to ensure connection stays open - OK
# SQL time format: `1999-12-31 23:59:59.99'
# Row contents: collection_time, gauge, sensor, value
    obs1, obs2 = zip(*[('2001-10-01 12:45:00', '2017-01-20 00:20:00'),
                       ('Buchanan Dam', 'Stream at brushy creek'),
                       ('STAGE', 'tail'),
                       (10, 0.0005)
                      ]
                    )
    # md.insert_gauge_readings(obs1)
    # md.insert_gauge_readings(obs2)
    multiobs = [obs1, obs2]
    # md.insert_gauge_readings(multiobs)


    print len(md.query_max_precip())
