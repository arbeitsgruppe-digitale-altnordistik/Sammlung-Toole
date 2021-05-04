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
from handrit_tamer import get_data_from_browse_list as multiBrowse



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

# These three functions can be used as wrapper functions to redirect prints from terminal to the
# web interface. This will not work directly with cached functions.



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


def adv_options():
    '''Shows the advanced options menu'''

    st.title("Advanced Options Menu")
    st.write("Carefull! Some of these options can take a long time to complete! Like, a loooong time!")
    st.warning("There will be no confirmation on any of these! Clicking any of the option without thinking first is baaad juju!")
    collections_button()
    msNumber_button()
    rebuild_button()
    # generate_reports()


def search_page():
    '''Workbench. Proper doc to follow soon.'''

    st.title("Result Workflow Builder")
    if state.didRun == 'Started, dnf.':
        st.write('There was an issue last time')
        if st.button('Restart'):
            state.CurrentStep = 'Preprocessing'
            state.didRun = ''
    if state.CurrentStep == 'Preprocessing':
        st.header("Preprocessing")
        st.write("Construct your workflow with the options below. Instructions: For now, there are two input boxes: 1. For URLs pointing to a handrit search result page 2. For URLs pointing to a handrit browse result page.")
        state.currentSURL = st.text_input("Input handrit search URL here")
        state.multiSearch = st.checkbox("Do you want to process multiple URLs?", value=False, help="Please make sure to check this box if you want to process more than one URL at once", key="0.asdf")
        state.currentBURL = st.text_input("Input handrit browse URL here")
        state.multiBrowse = st.checkbox("Do you want to process multiple URLs?", value=False, help="Please make sure to check this box if you want to process more than one URL at once", key='1.asdf')
        state.resultMode = st.radio("Select the type of information you want to extract", ['Contents', 'Metadata'], index=0)
        state.joinMode = st.radio("Show only shared or all MSs?", ['Shared', 'All'], index=1)
        if st.button("Run"):
            state.didRun = 'Started, dnf.'
            state.CurrentStep = 'Processing'
            # This block handles data delivery
            if state.currentSURL and state.multiSearch == False:
                dataS = search_results(state.currentSURL, state.resultMode)
            if state.currentSURL and state.multiSearch == True:
                baseList = [x.strip() for x in state.currentSURL.split(',')]
                dataS = multiSearch(baseList, DataType=state.resultMode, joinMode=state.joinMode)
            if state.currentBURL and state.multiBrowse == False:
                dataB = browse_results(inURL=state.currentBURL, DataType=state.resultMode)
            if state.currentBURL and state.multiBrowse == True:
                baseList = [x.strip() for x in state.currentBURL.split(',')]
                dataB = multiBrowse(baseList, state.resultMode, state.joinMode)
            
            # This block will check the data the got delivered and display it
            if state.joinMode == 'Shared':
                if state.resultMode == 'Metadata':
                    if state.currentSURL and state.currentBURL:
                        state.currentData = pd.merge(dataS, dataB, how='inner', on='Handrit-ID')
                    if state.currentSURL and not state.currentBURL:
                        state.currentData = dataS
                    if state.currentBURL and not state.currentSURL:
                        state.currentData = dataB
                else:
                    if state.currentSURL and state.currentBURL:
                        mssA = list(dataS.columns.values)
                        mssB = list(dataB.columns.values)
                        mssC = [i for i in mssA if i in mssB]
                        middelData = pd.concat([dataS, dataB], axis=1)
                        state.currentData = middelData[middelData.columns.intersection(mssC)].drop_duplicates()
                    if state.currentSURL and not state.currentBURL:
                        state.currentData = dataS
                    if state.currentBURL and not state.currentSURL:
                        state.currentData = dataB
            if state.joinMode == 'All':
                if state.resultMode == 'Metadata':
                    if state.currentSURL and state.currentBURL:
                        state.currentData = pd.concat([dataS, dataB], axis=0).drop_duplicates().reset_index(drop=True)
                    if state.currentSURL and not state.currentBURL:
                        state.currentData = dataS
                    if state.currentBURL and not state.currentSURL:
                        state.currentData = dataB
                else:
                    if state.currentSURL and state.currentBURL:
                        state.currentData = pd.concat([dataS, dataB], axis=1)
                    if state.currentSURL and not state.currentBURL:
                        state.currentData = dataS
                    if state.currentBURL and not state.currentSURL:
                        state.currentData = dataB
            if not state.currentData.empty:
                state.didRun = 'OK'   
        if state.didRun == 'OK':
            st.header('Results')
            st.write(state.currentData)
    if state.didRun == 'OK': 
        if st.button("Go to postprocessing"):
            state.CurrentStep = 'Postprocessing'
    if state.CurrentStep == 'Postprocessing':
        postprocessing()
        if st.button("Go back to preprocessing"):
            state.CurrentStep = 'Preprocessing'
        


def citaviExporter():
    if state.resultMode == 'Contents':
        foundListList = list(state.currentData.columns)
        foundList = [i for x in foundListList for i in x]
    if state.resultMode == 'Metadata':
        foundList = state.currentData['Handrit-ID'].values.tolist()
    state.CitaviSelect = st.multiselect(label="Select which MSs you want to export to Citavi", options=foundList, help="This will export your selected references as a CSV file for Citavi.")
    if st.button('Export'):
        state.currentCitaviData, sink = metadata.get_citavified_data(inData=state.CitaviSelect, DataType='ids')
        st.write(state.currentCitaviData)
        csv = state.currentCitaviData.to_csv(sep='\t', encoding='utf-8', index=False)
        b64 = base64.b64encode(csv.encode("UTF-8")).decode()  # some strings <-> bytes conversions necessary here
        href = f'<b> There is a bug! Use "Right Click -> Save As" or it will break!</b><br /><a href="data:file/csv;base64,{b64}">Download CSV File</a><br /> (This is a raw file. You need to give it the ending .csv, the easiest way is to right-click the link and then click Save as or Save link as, depending on your browser.)'
        st.markdown(href, unsafe_allow_html=True)


def postprocessing():
    st.header("Postprocessing menu")
    st.header("Current result data set")
    st.write(state.currentData)
    if st.button("Export to CSV"):
        state.postStep = 'CSV'
    if state.postStep == 'CSV':
        csv = state.currentData.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        href = f'<b> There is a bug! Use "Right Click -> Save As" or it will break!</b><br /><a href="data:file/csv;base64,{b64}">Download CSV File</a><br /> (This is a raw file. You need to give it the ending .csv, the easiest way is to right-click the link and then click Save as or Save link as, depending on your browser.)'
        st.markdown(href, unsafe_allow_html=True)
    if st.button("Export references to Citavi"):
        state.postStep = 'Citavi'
    if state.postStep == 'Citavi':
        citaviExporter()
    if st.button("Clean data"):
        state.postStep = 'Cleaning'
    if state.postStep == 'Cleaning':
        dataCleaner()
    if state.resultMode == "Metadata":
        if st.button('Plot dating'):
            state.postStep = 'Plotting'
    if state.postStep == 'Plotting':
            fig = date_plotting(state.currentData)
            st.plotly_chart(fig, use_container_width=True)


# def dataInspector():


def date_plotting(inDF):
    ''' Plots the data of a given set of MSs. Used with MS metadata results. Returns scatterplot.

    Args:
        inDF(dataFrame, required): pandas DataFrame

    Returns:
        scatterplot data for plotly to be drawn with corresponding function
    '''

    inDF = inDF[inDF['Terminus ante quem'] != 0]
    inDF = inDF[inDF['Terminus post quem'] != 0]
    fig = px.scatter(inDF, x='Terminus post quem', y='Terminus ante quem', color='Signature')
    return fig


def dataCleaner():
    state.currentData = state.currentData.replace('None', np.nan)
    if state.resultMode == 'Metadata':
        index = state.currentData.index
        itemsPrev = len(index)
        newDF = state.currentData.dropna(axis=1, how='all')
        index1 = newDF.index
        itemsAfter = len(index1)
        diff = itemsPrev - itemsAfter
        st.write(f"Started out with {itemsPrev}, left with {itemsAfter}. Found {diff} NaN values.")
    else:
        itemsPrev = len(state.currentData.columns)
        newDF = state.currentData.dropna(axis=1, how='all')
        itemsAfter = len(newDF.columns)
        newDF = newDF.loc[:,~newDF.columns.duplicated()]
        itemsAfter1 = len(newDF.columns)
        diff0 = itemsPrev - itemsAfter
        diff1 = itemsAfter - itemsAfter1
        st.write(f"Started out with {itemsPrev} results, dropped {diff0} NaN values, dropped {diff1} duplicates. Remaining unique results: {itemsAfter1}")
    st.write(newDF)
    if st.button("Keep cleaned data"):
        state.currentData = newDF

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
    state = sessionState.get(currentData=pd.DataFrame(),
                            resultMode='', 
                            currentSURL='', 
                            currentBURL='', 
                            URLType='', 
                            multiSearch='False', 
                            multiBrowse='False', 
                            joinMode='All',
                            didRun='dnr',
                            CitaviSelect=[],
                            CurrentStep = 'Preprocessing',
                            postStep = '',
                            currentCitaviData = pd.DataFrame()
                            )
    full_menu()