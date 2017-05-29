"""WebScraping methods for lcra hydromet site.

Data will be held in memory, then inserted into a SQL database
using methods from another module.

Requires selenium webdriver for chrome.

Due to the nested nature of the scraper, functions are ordered
in descending order by nest-level in the code.
"""

# Tools for building element loops
from bs4 import BeautifulSoup
import requests

# scipy
import pandas as pd
import numpy as np

# Web Driver Tools
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException

# Python
from time import sleep
from datetime import date, timedelta, datetime

# SQL module
from sql_class import ManipulateDatabase


class HLScraper(object):

    def __init__(self,
                 url='http://hydromet.lcra.org/chronhist.aspx',
                 current_gauge_value=None,
                 start_date='01/01/2001',
                 end_date=None):

        self.url = url
        self.current_gauge_value = current_gauge_value
        self.start_date = self._conv_date(start_date)
        if not end_date:
            self.end_date = date.today()
        else:
            self.end_date = self._conv_date(end_date)
        self.driver = None
        self.gauge_list = None
        self.cur_gauge = None


    def start(self,
              url='http://hydromet.lcra.org/chronhist.aspx',
              current_gauge_value=None):
        """Initializes chrome using the selenium driver

        start_gauge_value - Gauge ID of the gauge to begin scraping
                            loop at. Assumes a static gauge list order

        It may help to run this function with a UNIX redirection
        for STDOUT to log the behavior
        """
	print "Start Scraper"
        self.get_remaining_gauge_list(self.current_gauge_value)

        self.driver = webdriver.Chrome()
        self.driver.implicitly_wait(30)
        self.driver.get(self.url)

        self._cycle_gauges()


    def quit(self):
        self.driver.quit()


    def get_remaining_gauge_list(self, current_gauge_value=None):
        """Returns a subset of the gauge list beginning at
        current_gauge_value
        If gauge_value == None, returns full gauge list"""

        gauge_list = self._get_gauge_list()

        if current_gauge_value:
            idx = zip(*gauge_list)[0].index(str(current_gauge_value))
            self.gauge_list = gauge_list[idx:]
        else:
            self.gauge_list = gauge_list


    def _get_gauge_list(self):
        """Returns a list of gauge values/names.

        INPUT:
            string  (url)  | URL of the site

        OUTPUT:
            list           | list of tuples containing strings
                             (gauge number, gauge name)
        """

        r = requests.get(self.url)
        soup = BeautifulSoup(r.content, 'html.parser')
        dl1 = soup.find(id='DropDownList1')
        allgauges = dl1.find_all("option")

        return [(g.get('value'), g.contents[0]) for g in allgauges]


    def _cycle_gauges(self):
        """Cycles through the gauges in the gauge list.
        Calls the _cycle_options function to cycle through sensors
        """

        for gaugevalue, gaugename in self.gauge_list:
            select = Select(
                     self.driver.find_element_by_name('DropDownList1')
                           )
            print "-" * 70
            print "-" * 70
            print "-" * 70
            print "selected \t- DropDownList1"
            select.select_by_value(gaugevalue)
            self.cur_gauge = gaugevalue
            print "clicked \t- {:15} {:15}".\
                format(gaugevalue, gaugename)
            print "-" * 70
            print "-" * 70

            self._cycle_options()


    def _cycle_options(self):
        """Cycles options in DropDownList2 and calls remaining
        methods
        """
        sensor_options = self._get_sensor_options()

        for optionvalue, optionname in sensor_options:
            select = Select(
                     self.driver.find_element_by_name('DropDownList2')
                           )
            select.select_by_value(optionvalue)
            # option.click()
            print "-" * 70
            print "\nselected \t- DropDownList2"
            print "clicked \t- {:15} {:15}\n".\
                format(optionvalue, optionname)
            print "-" * 70
            self._cycle_dates()


    def _get_sensor_options(self):
        """Get all sensor options currently displayed
        """
        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        dl2 = soup.find(id=('DropDownList2'))
        alloptions = dl2.find_all("option")

        return [(o.get('value'), o.contents[0]) for o in alloptions]


    def _cycle_dates(self):
        """Cycles through date options"""
        drang = timedelta(days=179)  ##### MAY BE ATTR IN FUTURE
        cur_end_date = self.end_date
        cur_start_date = cur_end_date - drang

        while cur_end_date >= self.start_date:
            self._enter_dates(cur_start_date, cur_end_date)

            submit = self.driver.find_element_by_name("Button1")
            submit.click()
            print "clicked \t- Button1"

            flag = self._parse_table()
            if flag == "break":
                break

            cur_end_date = cur_start_date - timedelta(1) #inclusive
            cur_start_date = max(cur_end_date - drang, self.start_date)


    def _enter_dates(self, start, end):
        start_date_field = self.driver.find_element_by_name("Date1")
        end_date_field = self.driver.find_element_by_name("Date2")

        start_date_field.clear()
        start_date_field.send_keys(self._conv_date(start))
        end_date_field.clear()
        end_date_field.send_keys(self._conv_date(end))

        print "selected \t- Date2"
        print "entered \t- {:15} {:15}".\
            format("End Date", self._conv_date(end))
        print "selected \t- Date1"
        print "entered \t- {:15} {:15}".\
            format("Start Date", self._conv_date(start))


    def _parse_table(self):
        """Check if table exists, then parse using bs4"""
        try:
            table = self.driver.find_element_by_tag_name("tbody")
        except NoSuchElementException:
            print "exception \t- tbody not found"
            return "break"

        soup = BeautifulSoup(self.driver.page_source, 'lxml')
        tbody = soup.find("tbody")

        rows = tbody.children
        headers = [header.string for header in rows.next().children][1:-1]

        inserts = []
        for row in list(rows)[:-1]:
            values = [v.string for v in row.children][1:-1]

            for val, head in zip(values[1:], headers[1:]):
                inserts.append((values[0], 'gauge', head, float(val)))
        print "parsed \t\t- tbody"

        self._sql_entry(inserts)


    def _sql_entry(self, inserts):

        md = ManipulateDatabase()
        md.insert_gauge_readings(inserts)

        now = datetime.now().strftime("%m/%d/%y %H:%m:%S")
        print "access time\t- {}\n".format(now)
        print "inserted \t- {} records".format(len(inserts))

    def _conv_date(self, dt):
        """Convert a string of m/d/Y to date object
        dstr > date string
        or vice versa
        """

        if isinstance(dt, str):
            m, d, y = int(dt[:2]), int(dt[3:5]), int(dt[6:])
            return date(y, m, d)
        else:
            return dt.strftime("%m/%d/%Y")


if __name__ == "__main__":
    hls = HLScraper(current_gauge_value=None,
                    start_date='01/01/1950',
                    end_date=None)
    hls.start()

    hls.quit()


    #  TEST get_gauge_list() - OK
    # gauge_list = _get_gauge_list()
    # print gauge_list
    # print len(gauge_list) #consistently 163

    # TEST remaining_gauge_list - OK
    # remaining_gauge_list = get_remaining_gauge_list(2868)
    # remaining_gauge_list = get_remaining_gauge_list()
    # print remaining_gauge_list

    # TEST run_selenium with _cycle_options to print - OK
    # TEST pull webdriver from element - OK
    # TEST sendkeys in jupyter - OK


    # REFACTOR INTO CLASS
    # hls = HLScraper(current_gauge_value=3330,
    #                 start_date='01/01/1950',
    #                 end_date=None)


    # Test date and time conversion functions
    # print "Convert '01/20/2001'"
    # print hls._conv_date("01/20/2001")
    # print "*" * 50
    # datetest = date(2007, 12, 5)
    # print "datetest {}".format(datetest)
    # print hls._conv_date(datetest)
