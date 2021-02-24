import streamlit as st
import pandas as pd
import numpy as np
# import handrit_tamer as tame
import crawler
import csv
import time


# Constants
# ---------

_coll_path = 'data/collections.csv'
_id_path = 'data/ms_ids.csv'


# Functions which make the page do stuff

def rebuild_button():
    if st.sidebar.button("Rebuild meta-cache"):
        st.spinner("In Progress")
        start = time.time()
        crawler.get_collections(use_cache=False)
        crawler.get_ids(use_cache=False)
        crawler.get_xml_urls(use_cache=False)
        end = time.time()
        duration = end - start
        print(f"It took {duration} to rebuild meta-cache!")


def redo_xmls_button():
    if st.sidebar.button("Rebuild XML cache"):
        start = time.time()
        noMSs = crawler.cache_all_xml_data(aggressive_crawl=True, use_cache=False)
        end = time.time()
        duration = end - start
        print(f"Downloaded {noMSs} XML files in {duration}!")

def msNumber_button():
    if st.sidebar.button("Show number of MS IDs cached"):
        with open(_id_path) as m:
            MScount = sum(1 for row in m)
        st.write(f"There are currently {MScount:,d} manuscript IDs in cache!")


def collections_button():
    if st.sidebar.button("Show all collections"):
        cols = crawler.get_collections()
        st.write(cols)


# Actual content and layout of the page
# -------------------------------------

st.write("Welcome to Sammlung Toole!")

collections_button()
msNumber_button()
redo_xmls_button()
rebuild_button()