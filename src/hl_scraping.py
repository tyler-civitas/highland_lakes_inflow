"""WebScraping methods for lcra hydromet site.

Data will be held in memory, then inserted into a SQL database
using methods from another module.

Requires selenium webdriver for chrome
"""

# Tools for building element loops
from BeautifulSoup import BeautifulSoup
import requests

# scipy
import pandas as pd
import numpy as np

# Web Driver Tools
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from time import sleep



def get_gauge_list(url='http://hydromet.lcra.org/chronhist.aspx'):
    """Returns a list of gauge values/names.

    INPUT:
        string  (url)  | URL of the site

    OUTPUT:
        list           | list of tuples containing strings of format
                         (gauge number, gauge name)
    """
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser')

    dl1 = soup.find(id='DropDownList1')
    allgauges = dl1.find_all("option")

    gaugevalues = []
    gaugenames = []

    for gauge in allgauges:
        gaugevalues.append(gauge.get('value'))
        gaugenames.append(gauge.contents[0])


    return zip(gaugevalues, gaugenames)


if __name__ == "__main__":

    gauge_list = get_gauge_list()
