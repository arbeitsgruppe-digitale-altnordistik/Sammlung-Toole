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

_coll_path = 'data/collections.csv' # Contains the different collections from handrit
_id_path = 'data/ms_ids.csv'        # Contains XML IDs
_xml_path = 'data/xml/'             # Directory where handrit XMLs are stored
_date_path = 'data/dating_all.csv'  # CSV containing the datings of all MSs
_big_plot = 'data/dating_all.png'   # Scatter plot of the above dating data (static)
_home_image = 'data/title.png'      # Image displayed on home page


# System
# ------
'''
These three functions can be used as wrapper functions to redirect prints from terminal to the
web interface. This will not work directly with cached functions.
'''


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
    ''' This will run the crawl() function from the crawler, which will download everything
    from handrit
    '''
    if st.sidebar.button("Download everything"):
        with st.spinner("In Progress"):
            st.write(f'Start: {datetime.now()}')
            with st_stdout('code'):
                crawler.crawl(use_cache=False)
        st.write(f'Finished: {datetime.now()}')


def redo_xmls_wrap():
    ''' Wrapper for XML redo function to redirect output to web interface.
    '''
    with st_stdout('code'):
        noMSs = crawler.cache_all_xml_data(aggressive_crawl=True, use_cache=False)
    return noMSs


def msNumber_button():
    ''' Displays the button that shows the number of MS IDs cached. Not strictly necessary.
    '''
    if st.sidebar.button("Show number of MS IDs cached"):
        with open(_id_path) as m:
            MScount = sum(1 for row in m)
        st.write(f"There are currently {MScount:,d} manuscript IDs in cache!")


def collections_button():
    '''Self explanatory
    '''
    if st.sidebar.button("Show all collections"):
        cols = crawler.get_collections()
        st.write(cols)


def search_input():
    '''Serves the contents of the Handrit Search option. Takes a search URL as input from the user,
    can handle multiple URLs separated by comma.
    '''
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


@st.cache(suppress_st_warning=True) # -> won't work with stdout redirect
def search_results(inURL: str, DataType: str):
    ''' Actual call to handrit tamer to get the desired results from the search URL.

    The data frame to be returned depends on the DataType variable (cf. below).
    If DataType = Contents:
        Data frame columns will be the shelfmarks/IDs of the MSs, each column containing the text
        witnesses listed in the MS description/XML.

    If DataType = Metadata:
        Data frame contains the following columns:
        ['Handrit ID', 'Signature', 'Country',
                               'Settlement', 'Repository', 'Original Date', 'Mean Date', 'Range']
    
    Args:
        inURL(str, required): A URL pointing to a handrit search result page.
        DataType(str, required): Whether you want to extract the contents of MSs from the XMLs or metadata
        such as datings and repository etc. (cf. above). Can be 'Contents' or 'Metadata'

    Returns:
        pd.DataFrame: DataFrame containing MS contents or meta data.
    '''
    data = hSr(inURL, DataType)    
    return data


def browse_input():
    '''Serves content of the Handrit Browse option, taking a URL of a handrit browse page as input
    in the text input box below
    '''
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


@st.cache(suppress_st_warning=True)
def browse_results(inURL: str, DataType: str):
    ''' Actual call to handrit tamer to get the desired results from the browse URL.

    The data frame to be returned depends on the DataType variable (cf. below).
    If DataType = Contents:
        Data frame columns will be the shelfmarks/IDs of the MSs, each column containing the text
        witnesses listed in the MS description/XML.

    If DataType = Metadata:
        Data frame contains the following columns:
        ['Handrit ID', 'Signature', 'Country',
                               'Settlement', 'Repository', 'Original Date', 'Mean Date', 'Range']
    
    Args:
        inURL(str, required): A URL pointing to a handrit search result page.
        DataType(str, required): Whether you want to extract the contents of MSs from the XMLs or metadata
        such as datings and repository etc. (cf. above). Can be 'Contents' or 'Metadata'

    Returns:
        pd.DataFrame: DataFrame containing MS contents or meta data.
    '''
    data = hBr(inURL, DataType)
    return data


def date_plotting(inDF):
    ''' Plots the data of a given set of MSs. Used with MS metadata results. Returns scatterplot.

    Args:
        inDF(dataFrame, required): pandas DataFrame

    Returns:
        scatterplot data for plotly to be drawn with corresponding function
    '''
    inDF = inDF[inDF['Terminus Antequem'] != 0]
    inDF = inDF[inDF['Terminus Postquem'] != 0]
    fig = px.scatter(inDF, x='Terminus Postquem', y='Terminus Antequem', color='Signature')
    return fig


def generate_reports():
    '''This is to be used for all sorts of reports which can be generated from the data.
    As of yet only has one option: Scatter plot of all MSs dating info. Takes a few minutes.
    Doesn't need anything, calls the date_extractor module.
    '''
    if st.sidebar.button("Generate Reports"):
        st.write("Statically generate expensive reports. The results will be stored in the data directory. Running these will take some time.")
        with st.spinner("In progress"):
            st.write(f'Start: {datetime.now()}')
            with st_stdout('code'):
                date_extractor.do_plot(use_cache=False)
            st.write(f'Finished: {datetime.now()}')


def all_MS_datings():
    ''' Displays a previously rendered scatter plot of all MS dating info and the corresponding DF
    from a csv.
    '''
    st.write("Displaying scatter plot of all available MS dates (post- and antequem) below")
    inDF = pd.read_csv(_date_path).drop_duplicates(subset='Shelfmark')
    st.write(inDF)
    st.image(_big_plot)



# Functions which create sub pages
# --------------------------------------


def mainPage():
    '''Landing page'''
    st.title("Welcome to Sammlung Toole")
    st.write("The Menu on the left has all the options")
    st.image(_home_image)
    st.balloons()


def adv_options():
    '''Shows the advanced options menu'''
    st.title("Advanced Options Menu")
    st.write("Carefull! Some of these options can take a long time to complete! Like, a loooong time!")
    st.warning("There will be no confirmation on any of these! Clicking any of the option without thinking first is baaad juju!")
    collections_button()
    msNumber_button()
    rebuild_button()
    generate_reports()


def search_page():
    '''Basic page for handrit search/browse operations. Only used to select either search or browse'''
    st.title("Search Page")
    st.write("Different search options")
    searchOptions = {"Handrit Browse": "browse_input", "Handrit Search": "search_input"}
    selection = st.sidebar.radio("Search Options", list(searchOptions.keys()), index=0)
    selected = searchOptions[selection]
    st.write(f"You chose {selection}")
    eval(selected + "()")


def static_reports():
    '''Page for expensive reports. As of yet only contains one item. Can be expanded later'''
    reports = {"Dating of all MSs": "all_MS_datings"}
    selection = st.sidebar.radio("Select report to display", list(reports.keys()), index=0)
    selected = reports[selection]
    eval(selected + "()")


# Menu Functions
# --------------


def full_menu():
    '''This is basically the main() and will load and display the full menu, which in turn calls
    all the other functions containing sub pages.
    '''
    MenuOptions = {"Home": "mainPage", "Search Functions": "search_page", "Reports": "static_reports", "Advanced Settings": "adv_options"}
    selection = st.sidebar.selectbox("Menu", list(MenuOptions.keys()))
    selected = MenuOptions[selection]
    eval(selected + "()")


# Run
#----

full_menu()