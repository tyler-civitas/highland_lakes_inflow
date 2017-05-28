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

# Python
from time import sleep

from datetime import date, timedelta


class HLScraper(object):

    def __init__(self,
                 url='http://hydromet.lcra.org/chronhist.aspx',
                 current_gauge_value=None,
                 start_date='01/01/2001',
                 end_date=None):

        self.url = url
        self.current_gauge_value = current_gauge_value
        self.start_date = start_date
        if not end_date:
            self.end_date = date.today().strftime("%m/%d/%Y")
        self.end_date = end_date
        self.driver = None
        self.gauge_list = None


    def start(self,
              url='http://hydromet.lcra.org/chronhist.aspx',
              current_gauge_value=None):
        """Initializes chrome using the selenium driver

        start_gauge_value - Gauge ID of the gauge to begin scraping
                            loop at. Assumes a static gauge list order

        It may help to run this function with a UNIX redirection
        for STDOUT to log the behavior
        """
        self.get_remaining_gauge_list(self.current_gauge_value)

        self.driver = webdriver.Chrome()
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
            print "selected \t- DropDownList1"
            select.select_by_value(gaugevalue)
            print "clicked \t- {:15} {:15}".\
                format(gaugevalue, gaugename)

            select_sensor = \
             Select(self.driver.find_element_by_name('DropDownList2'))
            print "\nselected \t- DropDownList2"
            self._cycle_options(select_sensor.options)
            print "-" * 70


    def _cycle_options(self, sensor_options):
        """Cycles options in DropDownList2 and calls remaining
        methods
        """

        for option in sensor_options:
            option.click()
            print "clicked \t- {:15} {:15}".\
                format(option.get_attribute("value"), option.text)
            self._cycle_dates(option)


    def _cycle_dates(self, option):
        """Cycles through date options"""
        start_date_field = self.driver.find_element_by_name("Date1")
        end_date_field = self.driver.find_element_by_name("Date2")

        daylimit = timedelta(days=179)  ##### MAY BE ATTR IN FUTURE

        date1 = '01/01/2001'
        start_date_field.clear()
        end_date_field.send_keys(date1)
        print "selected \t- Date1"
        print "entered \t- {:15}".format(date1)


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
    pass

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
    hls = HLScraper(current_gauge_value=2868,
                    start_date='01/01/1950',
                    end_date=date.today())


    # Test date and time conversion functions
    # print "Convert '01/20/2001'"
    # print hls._conv_date("01/20/2001")
    # print "*" * 50
    # datetest = date(2007, 12, 5)
    # print "datetest {}".format(datetest)
    # print hls._conv_date(datetest)


    # hls.start()
    #
    # hls.quit()
