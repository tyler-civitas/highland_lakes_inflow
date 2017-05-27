"""
Contains class ManipulateDatabase

ManipulateDatabase can be used to connect to SQL server and insert
rows.

__main__ block is used for testing
"""

import psycopg2
from psycopg2 import sql


class ManipulateDatabase(object):
    """
    An object used to manipulate the postgres database
    """

    def __init__(self, user='tyler', db='highland_lakes'):
        self.user = user
        self.db = db
        self.conn = psycopg2.connect(dbname=self.db,
                                     user=self.user,
                                     host='localhost',
                                     password=''
                                    )


    def create_table(self):
        """Create the table from scratch"""

        if self._check_table_existence():
            raise RuntimeWarning("Table exists! Drop it first!")
        cur = self.conn.cursor()
        cur.execute('''CREATE TABLE hydromet
                              (collection_time TIMESTAMP PRIMARY KEY,
                               gauge TEXT,
                               sensor TEXT,
                               value REAL
                              );'''
                   )
        self.conn.commit()
        cur.close()


    def insert_gauge_reading(self, observation):
        """Insert a single reading from an observation tuple

        Format: (timestamp, gauge, sensor, value)

        NOTE: Some entries in hydromet have multiple values, such as
        'count', 'rainfall'. These should be stored as seperate
        entries. Sensor is the table header not the page element.

        Table name is hard-coded for parameterization
        """

        collection_time, gauge, sensor, value = observation
        cur = self.conn.cursor()
        cur.execute('''INSERT INTO hydromet
                       (collection_time, gauge, sensor, value)
                       VALUES (%s, %s, %s, %s)''',
                    (collection_time, gauge, sensor, value)
                   )
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



# POOR MANS UNIT TESTING ENVIRONMENT
if __name__ == "__main__":

    # TEST Class Instantiation - OK
    md = ManipulateDatabase()

    # TEST Table Creation, TEST _check_table_existence() - OK
    # md.create_table()

    # TEST insert_gauge_reading - OK
    # TEST multiple commits to ensure connection stays open - OK
    # SQL time format: `1999-12-31 23:59:59.99'
    # Row contents: collection_time, gauge, sensor, value
    # obs1, obs2 = zip(*[('2001-10-01 12:45:00', '2017-01-20 00:20:00'),
    #                    ('Buchanan Dam', 'Stream at brushy creek'),
    #                    ('STAGE', 'tail'),
    #                    (10, 0.0005)
    #                   ]
    #                 )
    # md.insert_gauge_reading(obs1)
    # md.insert_gauge_reading(obs2)
