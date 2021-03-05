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


# Constants
# ---------

_coll_path = 'data/collections.csv'
_id_path = 'data/ms_ids.csv'

# Utility Functions
# -----------------


def rebuild_button():
    if st.sidebar.button("Rebuild meta-cache"):
        st.spinner("In Progress")
        start = time.time()
        crawler.get_collections(use_cache=False)
        crawler.get_ids(use_cache=False)
        crawler.get_xml_urls(use_cache=False)
        end = time.time()
        duration = end - start
        st.write(f"It took {duration} to rebuild meta-cache!")


def redo_xmls_button():
    if st.sidebar.button("Rebuild XML cache"):
        st.write("This will download ALL XMLs from handrit.is and overwrite any and existing files. Do you want to continue? Warning! This will take somewhere between 30 minutes and several hours!")
        if st.button("No"):
            return
        if st.button("Yes"):
            st.spinner("In Progress")
            start = time.time()
            noMSs = crawler.cache_all_xml_data(aggressive_crawl=True, use_cache=False)
            end = time.time()
            duration = end - start
            st.write(f"Downloaded {noMSs} XML files in {duration}!")


def msNumber_button():
    if st.sidebar.button("Show number of MS IDs cached"):
        with open(_id_path) as m:
            MScount = sum(1 for row in m)
        st.write(f"There are currently {MScount:,d} manuscript IDs in cache!")


def collections_button():
    if st.sidebar.button("Show all collections"):
        cols = crawler.get_collections()
        st.write(cols)


def test_button():
    if st.sidebar.button("Test this shit"):
        st.write("Fuck!")


def search_input():
    inURL = st.text_input("Input search URL here")
    dataType = st.radio("Select the type of information you want to extract", ['Contents', 'Metadata'], index=0)
    data = search_results(inURL, dataType)
    st.write(data)
    if st.button("Export to CSV"):
        csv = data.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        href = f'<a href="data:file/csv;base64,{b64}">Download CSV File</a> (This is a raw file. You need to give it the ending .csv, the easiest way is to right-click the link and then click Save as or Save link as, depending on your browser.)'
        st.markdown(href, unsafe_allow_html=True)

@st.cache(suppress_st_warning=True)
def search_results(inURL, dataType):
    data = hSr(inURL, dataType)    
    return data


def browse_input():
    inURL = st.text_input("Input browse URL here")
    dataType = st.radio("Select the type of information you want to extract", ['Contents', 'Metadata'], index=0)
    data = browse_results(inURL, dataType)
    st.write(data)
    if st.button("Plot dating"):
        if dataType == "Metadata":
            histo = np.histogram(data[['Terminus Postquem', 'Terminus Antequem']], bins='auto', range=(1200, 1900))
            st.bar_chart(histo)
        else:
            st.write("Only works with metadata selected above.")
    if st.button("Export to CSV"):
        csv = data.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        href = f'<a href="data:file/csv;base64,{b64}">Download CSV File</a> (This is a raw file. You need to give it the ending .csv, the easiest way is to right-click the link and then click Save as or Save link as, depending on your browser.)'
        st.markdown(href, unsafe_allow_html=True)


@st.cache(suppress_st_warning=True)
def browse_results(inURL: str, dataType: str):
    data = hBr(inURL, dataType)
    return data


# Functions which create fake sub pages
# --------------------------------------


def mainPage():
    st.title("Welcome to Sammlung Toole")
    st.write("The Menu on the left has all the options")

def adv_options():
    st.title("Advanced Options Menu")
    st.write("Carefull! Some of these options can take a long time to complete! Like, a loooong time!")
    collections_button()
    msNumber_button()
    redo_xmls_button()
    rebuild_button()
    test_button()

def search_page():
    st.title("Search Page")
    st.write("Different search options")
    searchOptions = {"Handrit Browse": "browse_input", "Handrit Search": "search_input"}
    selection = st.sidebar.radio("Search Options", list(searchOptions.keys()), index=0)
    selected = searchOptions[selection]
    st.write(f"You chose {selection}")
    eval(selected + "()")

# Menu Functions
# --------------


def full_menu():
    MenuOptions = {"Home": "mainPage()", "Advanced Settings": "adv_options()", "Search Functions": "search_page"}
    selection = st.sidebar.selectbox("Menu", list(MenuOptions.keys()))
    selected = MenuOptions[selection]
    eval(selected + "()")


# System settings
# ---------------





# Actual content and layout of the page
# -------------------------------------

if __name__ == "__main__":
    full_menu()
