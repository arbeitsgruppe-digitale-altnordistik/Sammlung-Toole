import sys
import requests
import lxml
from bs4 import BeautifulSoup
import urllib
import pandas as pd
import time
import crawler
from crawler import load_xml
from crawler import load_xmls_by_id
import os
import csv

charactercountlist = []
textcountlist = []
desccountlist = []
bibcountlist = []

def charactercounter():
    for url in testURLList:
        try:
            soup = crawler.load_xml(url)
            charactercount = len(str(soup))
            print(url, "character count", charactercount)
            charactercountlist.append(charactercount)
        except:
            pass

def textcounter():
    for url in testURLList:
        try:
            soup = crawler.load_xml(url)
            textcount = len(str(soup.get_text()))
            print(url, "text count", textcount)
            textcountlist.append(textcount)
        except:
            pass

def desccounter():
    for url in testURLList:
        try:
            soup = crawler.load_xml(url)
            alldesc = soup.find_all('physDesc')
            desccount = len(str(alldesc))
            print(url, "description count", desccount)
            desccountlist.append(desccount)
        except:
            pass

def bibcounter():
    for url in testURLList:
        try:
            soup = crawler.load_xml(url)
            bibcount = len(str(soup.find_all('listBibl')))
            print(url, "bibliography count", bibcount)
            bibcountlist.append(bibcount)
        except:
            pass


testURLList = ["https://handrit.is/is/manuscript/xml/Lbs08-0520-is.xml", "https://handrit.is/is/manuscript/xml/Lbs08-5167-is.xml", "https://handrit.is/is/manuscript/xml/AM04-0960-XV-is.xml", "https://handrit.is/is/manuscript/xml/AM02-0242-en.xml", "https://handrit.is/is/manuscript/xml/JS08-0381-is.xml", "https://handrit.is/is/manuscript/xml/GKS02-1005-is.xml"]
charactercounter()
textcounter()
desccounter()
bibcounter()
print("charactercountlist:", charactercountlist)
print("textcountlist:", textcountlist)
print("desccountlist:", desccountlist)
print("bibcountlist:", bibcountlist)