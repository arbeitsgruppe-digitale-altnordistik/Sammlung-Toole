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
import metadata
import sessionState



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
"""
These three functions can be used as wrapper functions to redirect prints from terminal to the
web interface. This will not work directly with cached functions.
"""


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

    st.title("Result Workflow Builder")
    st.write("Construct your workflow with the options below. Instructions: For now, there are two input boxes: 1. For URLs pointing to a handrit search result page 2. For URLs pointing to a handrit browse result page.")
    state.currentSURL = st.text_input("Input handrit search URL here")
    state.multiSearch = st.checkbox("Do you want to process multiple URLs?", value=False, help="Please make sure to check this box if you want to process more than one URL at once", key="0.asdf")
    state.currentBURL = st.text_input("Input handrit browse URL here")
    state.multiBrowse = st.checkbox("Do you want to process multiple URLs?", value=False, help="Please make sure to check this box if you want to process more than one URL at once", key='1.asdf')
    state.resultMode = st.radio("Select the type of information you want to extract", ['Contents', 'Metadata', 'Maditadata'], index=0)
    state.joinMode = st.radio("Show only shared or all MSs?", ['Shared', 'All'], index=0)
    if st.button("Run"):
        if state.currentSURL and state.multiSearch == False:
            dataS = search_results(state.currentSURL, state.resultMode)
        if state.currentSURL and state.multiSearch == True:
            baseList = [x.strip() for x in state.currentSURL.split(',')]
            dataS = multiSearch(baseList, DataType=state.resultMode, joinMode=state.joinMode)
        if state.currentBURL and state.multiBrowse == False:
            dataB = browse_results(inURL=state.currentBURL, DataType=state.resultMode)
        if state.currentSURL and state.currentBURL:
            data = pd.concat([dataS, dataB], axis=1)
            st.write(data)
        if state.currentSURL and not state.currentBURL:
            st.write(dataS)
        if state.currentBURL and not state.currentSURL:
            st.write(dataB)




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
if __name__ == '__main__':
    state = sessionState.get(currentData='', 
                            resultMode='', 
                            currentSURL='', 
                            currentBURL='', 
                            URLType='', 
                            multiSearch='False', 
                            multiBrowse='False', 
                            joinMode='All'
                            )
    full_menu()