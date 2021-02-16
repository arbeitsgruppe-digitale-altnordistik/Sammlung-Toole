import streamlit as st
import pandas as pd
import numpy as np
# import handrit_tamer as tame
import crawler
import csv


# Constants
# ---------

_coll_path = 'data/collections.csv'
_id_path = 'data/ms_ids.csv'


# Functions which make the page do stuff

def rebuild_button():
    if st.sidebar.button("Rebuild cache"):
        st.spinner("In Progress")
        duration = crawler.rebuild_all()
        st.write(f"It took {duration} seconds to rebuild the Collections and MS-ID cache")


def msNumber_button():
    if st.sidebar.button("Show number of MS IDs cached"):
        with open(_id_path) as m:
            MScount = sum(1 for row in m)
        st.write(f"There are currently {MScount:,d} manuscript IDs in cache!")


# Actual content and layout of the page
# -------------------------------------

st.write("Hello World!")

rebuild_button()
msNumber_button()