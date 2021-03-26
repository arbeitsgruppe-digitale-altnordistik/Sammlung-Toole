import streamlit as st
import pandas as pd
import numpy as np
import crawler
import csv
import time
import handrit_tamer as ht
from handrit_tamer import get_data_from_search_url as hSr
from handrit_tamer import get_data_from_browse_url as hBr
import base64
import matplotlib
import plotly.figure_factory as ff
from contextlib import contextmanager
from io import StringIO
from streamlit.report_thread import REPORT_CONTEXT_ATTR_NAME
from threading import current_thread
import sys
from bs4 import BeautifulSoup
import os
import date_extractor
import plotly.express as px
import streamlit.components.v1 as comps
from handrit_tamer import get_from_search_list as multiSearch
from datetime import datetime



# Constants
# ---------

_coll_path = 'data/collections.csv'
_id_path = 'data/ms_ids.csv'
_xml_path = 'data/xml/'
_date_path = 'data/dating_all.csv'
_html_path = 'data/dating_all.html'
_home_image = 'data/title.png'


# System
# ------

@contextmanager
def st_redirect(src, dst):
    placeholder = st.empty()
    output_func = getattr(placeholder, dst)

    with StringIO() as buffer:
        old_write = src.write

        def new_write(b):
            if getattr(current_thread(), REPORT_CONTEXT_ATTR_NAME, None):
                buffer.write(b)
                output_func(b)
            else:
                old_write(b)

        try:
            src.write = new_write
            yield
        finally:
            src.write = old_write


@contextmanager
def st_stdout(dst):
    with st_redirect(sys.stdout, dst):
        yield


@contextmanager
def st_stderr(dst):
    with st_redirect(sys.stderr, dst):
        yield



# Utility Functions
# -----------------


def rebuild_button():
    if st.sidebar.button("Download everything"):
        with st.spinner("In Progress"):
            st.write(f'Start: {datetime.now()}')
            with st_stdout('code'):
                crawler.crawl(use_cache=False)
        st.write(f'Finished: {datetime.now()}')


def redo_xmls_wrap():
    with st_stdout('code'):
        noMSs = crawler.cache_all_xml_data(aggressive_crawl=True, use_cache=False)
    return noMSs


def msNumber_button():
    if st.sidebar.button("Show number of MS IDs cached"):
        with open(_id_path) as m:
            MScount = sum(1 for row in m)
        st.write(f"There are currently {MScount:,d} manuscript IDs in cache!")


def collections_button():
    if st.sidebar.button("Show all collections"):
        cols = crawler.get_collections()
        st.write(cols)


def search_input():
    inURL = st.text_input("Input search URL here")
    DataType = st.radio("Select the type of information you want to extract", ['Contents', 'Metadata'], index=0)
    searchType = st.radio("Work with a single search or combine multiple searches? Separate multiple search result urls by comma.", ['Single', 'Multiple'], index=0)
    if searchType == "Multiple":
        joinMode = st.radio("Show only shared or all MSs?", ['Shared', 'All'], index=0)
    if not inURL:
        st.warning("No URL supplied!")
    if inURL and searchType == 'Single':
        data = search_results(inURL, DataType)
        st.write(data)
    if inURL and searchType == 'Multiple':
        baseList = [x.strip() for x in inURL.split(',')]
        data = multiSearch(baseList, DataType, joinMode)
        st.write(data)
    if DataType == "Metadata":
        if st.button("Plot dating"):
            fig = date_plotting(data)
            st.plotly_chart(fig, use_container_width=True)
    if st.button("Export to CSV"):
        csv = data.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        href = f'<a href="data:file/csv;base64,{b64}">Download CSV File</a> (This is a raw file. You need to give it the ending .csv, the easiest way is to right-click the link and then click Save as or Save link as, depending on your browser.)'
        st.markdown(href, unsafe_allow_html=True)


# @st.cache(suppress_st_warning=True) # -> won't work with stdout redirect
def search_results(inURL, DataType):
    data = hSr(inURL, DataType)    
    return data


def browse_input():
    inURL = st.text_input("Input browse URL here")
    DataType = st.radio("Select the type of information you want to extract", ['Contents', 'Metadata'], index=0)
    if not inURL:
        st.warning("No URL supplied!")
    if inURL:
        data = browse_results(inURL, DataType)
        st.write(data)
    if DataType == "Metadata":
        if st.button("Plot dating"):
            fig = date_plotting(data)
            st.plotly_chart(fig, use_container_width=True)
    if st.button("Export to CSV"):
        csv = data.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        href = f'<a href="data:file/csv;base64,{b64}">Download CSV File</a> This is a raw file. You need to give it the ending .csv, the easiest way is to right-click the link and then click Save as or Save link as, depending on your browser.'
        st.markdown(href, unsafe_allow_html=True)


# @st.cache(suppress_st_warning=True) -> currently not working
def browse_results(inURL: str, DataType: str):
    data = hBr(inURL, DataType)
    return data


def date_plotting(inDF):
    inDF = inDF[inDF['Terminus Antequem'] != 0]
    inDF = inDF[inDF['Terminus Postquem'] != 0]
    fig = px.scatter(inDF, x='Terminus Postquem', y='Terminus Antequem', color='Signature')
    return fig


def generate_reports():
    if st.sidebar.button("Generate Reports"):
        st.write("Statically generate expensive reports. The results will be stored in the data directory. Running these will take some time.")
        with st.spinner("In progress"):
            st.write(f'Start: {datetime.now()}')
            with st_stdout('code'):
                date_extractor.do_plot(use_cache=False)
            st.write(f'Finished: {datetime.now()}')



def all_MS_datings():
    st.write("Displaying scatter plot of all available MS dates (post- and antequem) below")
    inDF = pd.read_csv(_date_path).drop_duplicates(subset='Shelfmark')
    st.write(inDF)
    htmlf = open(_html_path, "r")
    
    comps.html(htmlf.read(), height=600)



# Functions which create sub pages
# --------------------------------------


def mainPage():
    st.title("Welcome to Sammlung Toole")
    st.write("The Menu on the left has all the options")
    st.image(_home_image)
    st.balloons()


def adv_options():
    st.title("Advanced Options Menu")
    st.write("Carefull! Some of these options can take a long time to complete! Like, a loooong time!")
    st.warning("There will be no confirmation on any of these! Clicking any of the option without thinking first is baaad juju!")
    collections_button()
    msNumber_button()
    rebuild_button()
    generate_reports()


def search_page():
    st.title("Search Page")
    st.write("Different search options")
    searchOptions = {"Handrit Browse": "browse_input", "Handrit Search": "search_input"}
    selection = st.sidebar.radio("Search Options", list(searchOptions.keys()), index=0)
    selected = searchOptions[selection]
    st.write(f"You chose {selection}")
    eval(selected + "()")

def static_reports():
    reports = {"Dating of all MSs": "all_MS_datings"}
    selection = st.sidebar.radio("Select report to display", list(reports.keys()), index=0)
    selected = reports[selection]
    eval(selected + "()")

# Menu Functions
# --------------


def full_menu():
    MenuOptions = {"Home": "mainPage", "Search Functions": "search_page", "Reports": "static_reports", "Advanced Settings": "adv_options"}
    selection = st.sidebar.selectbox("Menu", list(MenuOptions.keys()))
    selected = MenuOptions[selection]
    eval(selected + "()")


# Main
# ----

def main():
    full_menu()

# Run
#----

main()