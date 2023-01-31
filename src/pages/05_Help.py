import markdown
import streamlit as st

how_to = """
# Welcome to Ultima Toole!

## Contents
1. How to use
2. Data and sources
3. About


## How to Use
The most basic (and currently the only working) function is to take results from handrit.is and make the more accessible and useful.
You can search handrit using either the search function, via the search boxes on the handrit.is landing page, or via the browse funtion.
The browse function in handrit allows you to search for manuscripts based on a large selection of filters, from text-works contained to persons affiliated.
You can then copy the URL of the page displaying the result of your query and paste this URL into the textbox on the "Search" page here in Toole.
You can paste multple URLs, just be sure they are separated by a __comma__. You can also freely mix browse and search results.

Clicking on run will fetch your result lists from handrit and load the corresponding metadata from memory. When you are satisfied with the initial results, you 
can finetune them in postprocessing.

There are several options here:
1. Export to CSV
+
Will export the currently displayed data to a CSV file, which you can download.

2. Export References to Citavi
+
You can make a selection of MSs you want to export in a format suitable for easy import into Citavi.

3. Clean data
+
This function will remove duplicates and missing values from the result data set. You can save the cleaned data set or choose to keep working with
the initial data set.

4. Plot dating
+
Will create a scatter plot of all MSs, using terminus ante and post quem as the x and y axes.

## Data and sources
All source code is our own unless stated otherwise. All data and metadata is sourced from handrit.is with permission.
All data from handrit.is is subject to CC 4.0

## About
Sammlung Toole is developed by Arbeitsgruppe Digitale Arbeitsmethoden in der Altnordistik at the University of Basel.
Refer to https://github.com/arbeitsgruppe-digitale-altnordistik/Sammlung-Toole for the source code.
All source code is licensed under the MIT License. Source code, data and metadata which is not our own may be subject to other
license terms.


"""

st.markdown(how_to)
if st.button("Show detailed help on Citavi import/export"):
    with open('docs/CITAVI-README.md', 'r', encoding='utf-8') as citread:
        helpme = markdown.markdown(citread.read())
    st.markdown(helpme, unsafe_allow_html=True)
