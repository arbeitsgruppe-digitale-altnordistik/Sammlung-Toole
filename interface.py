import streamlit as st
import pandas as pd
import numpy as np
import crawler
import csv
import time
import options


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


# Menu Functions
# --------------


def full_menu():
    MenuOptions = {"Home": "mainPage()", "Advanced Settings": "adv_options()"}
    selection = st.sidebar.selectbox("Menu", list(MenuOptions.keys()))
    selected = MenuOptions[selection]
    eval(selected + "()")


# Actual content and layout of the page
# -------------------------------------

if __name__ == "__main__":
    full_menu()
