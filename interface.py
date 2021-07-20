from typing import Optional
import numpy as np
import streamlit as st
import pandas as pd
import util.crawler as crawler
import base64
from datetime import datetime
import markdown
import util.metadata as metadata
from util import sessionState
from util import utils
from util.constants import IMAGE_HOME
from util.stateHandler import StateHandler
from util.utils import Settings
from util.datahandler import DataHandler
from gui.guiUtils import Texts


log = utils.get_logger(__name__)
settings = Settings.get_settings()


def get_handler() -> None:
    if DataHandler.is_cached() or DataHandler.has_data_available():
        with st.spinner('Grabbing data handler...'):
            rebuild_handler()
    else:
        st.sidebar.text("No data at hand. Needs loading first.")
        adv_options()


def rebuild_all_button() -> None:
    ''' This will run the crawl() function from the crawler, which will download everything
    from handrit
    '''
    if st.sidebar.button("Download everything"):
        st.write(f'Start: {datetime.now()}')
        container = st.beta_container()
        xmls, contents = crawler.crawl(use_cache=False)
        st.write(f'Finished: {datetime.now()}')
        rebuild_handler(xmls, contents)


def reload_with_cache() -> None:
    st.write(f'Start: {datetime.now()}')
    container = st.beta_container()
    xmls, contents = crawler.crawl(use_cache=True)
    st.write(f'Finished: {datetime.now()}')
    rebuild_handler(xmls, contents)


def rebuild_handler(xmls: Optional[pd.DataFrame] = None, contents: Optional[pd.DataFrame] = None) -> None:
    st.write(f'Start: {datetime.now()}')
    container = st.beta_container()
    state.data_handler = DataHandler.get_handler(xmls=xmls, contents=contents)
    st.write(f'Finished: {datetime.now()}')
    st.experimental_rerun()
    # full_menu()


# Functions which create sub pages
# --------------------------------------


def mainPage() -> None:
    '''Landing page'''

    st.title("Welcome to Sammlung Toole")
    st.write("The Menu on the left has all the options")
    st.image(IMAGE_HOME)


def adv_options() -> None:
    '''Shows the advanced options menu'''
    # LATER: At some point we should consider changing crawling into a background task
    st.title("Advanced Options Menu")
    st.write("Carefull! Some of these options can take a long time to complete! Like, a loooong time!")
    st.warning("There will be no confirmation on any of these! Clicking any of the option without thinking first is baaad juju!")
    rebuild_all_button()
    if st.sidebar.button("Reload Missing Data"):
        reload_with_cache()
    if st.sidebar.button("Rebuild Data Handler"):
        rebuild_handler()
    if st.sidebar.button("Wipe cache"):
        crawler._wipe_cache()
    settings.max_res = st.sidebar.number_input("Maximum number of manuscripts to load",
                                               min_value=1,
                                               max_value=1000000,
                                               value=1000000)




def search_page() -> None:
    '''Workbench. Proper doc to follow soon.'''

    st.title("Result Workflow Builder")
    if state.CurrentStep == 'Preprocessing':
        st.header("Preprocessing")
        st.markdown(Texts.SearchPage.instructions)  # XXX: markdown not working here?
        state.currentURLs_str = st.text_area("Input handrit search or browse URL(s) here", help="If multiple URLs, put one URL per Line.")
       
        # state.resultMode = st.radio("Select the type of information you want to extract", ['Contents', 'Metadata'], index=0)
        # state.joinMode = st.radio("Show only shared or all MSs?", ['Shared', 'All'], index=1)
        if st.button("Run"):
            state.didRun = 'Started, dnf.'
            state.CurrentStep = 'Processing'
            # This block handles data delivery

            if state.currentURLs_str:
                s_urls = [url.strip() for url in state.currentURLs_str.split(',')]
                url_list, state.currentData = state.data_handler.get_ms_urls_from_search_or_browse_urls(urls=s_urls, sharedMode=(state.joinMode == 'Shared'))
                st.write("Processed Manuscript URLs:")
                st.write(url_list)  # TODO: give indication which strings are being watched, add "clear" button
                state.currentURL_list += url_list
                st.write("Overall MS URLs:")
                st.write(state.currentURL_list) # TODO: Required?

             
            if not state.currentData.empty:
                state.didRun = 'OK'
        if state.didRun == 'OK':
            st.header('Results')
            st.write(state.currentData)
    if state.didRun == 'OK':
        if st.button("Go to postprocessing"):
            state.CurrentStep = 'Postprocessing'
            state.didRun = None
            st.experimental_rerun()
    if state.CurrentStep == 'Postprocessing':
        postprocessing()
        if st.button("Go back to preprocessing"):
            state.CurrentStep = 'Preprocessing'
            state.currentData = None
            st.experimental_rerun()


def postprocessing() -> None:
    st.header("Postprocessing menu")
    st.header("Current result data set")
    st.write(state.currentData)
    if st.button("Export to CSV"):
        state.postStep = 'CSV'
    if state.postStep == 'CSV':
        csv = state.currentData.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        href = f'<a href="data:file/csv;base64,{b64}">Download CSV File</a> (This is a raw file. You need to give it the ending .csv, the easiest way is to right-click the link and then click Save as or Save link as, depending on your browser.)'
        st.markdown(href, unsafe_allow_html=True)
    if st.button("Export references to Citavi"):
        state.postStep = 'Citavi'
    if state.postStep == 'Citavi':
        citaviExporter()
    if st.button("Clean data"):
        state.postStep = 'Cleaning'
    if state.postStep == 'Cleaning':
        dataCleaner()
    if st.button('Plot dating'):
        state.postStep = 'Plotting'
    if state.postStep == 'Plotting':
        fig = utils.date_plotting(state.currentData)
        st.plotly_chart(fig, use_container_width=True)


def citaviExporter() -> None:
    foundListList = list(state.currentData.columns)
    foundList = [i for x in foundListList for i in x]
    state.CitaviSelect = st.multiselect(label="Select which MSs you want to export to Citavi", options=foundList,
                                        help="This will export your selected references as a CSV file for Citavi.")
    if st.button('Export'):
        state.currentCitaviData, _ = metadata.get_citavified_data(inData=state.CitaviSelect, DataType='ids')
        st.write(state.currentCitaviData)
        csv = state.currentCitaviData.to_csv(sep='\t', encoding='utf-8', index=False)
        b64 = base64.b64encode(csv.encode("UTF-8")).decode()  # some strings <-> bytes conversions necessary here
        href = f'<b> There is a bug! Use "Right Click -> Save As" or it will break!</b><br /><a href="data:file/csv;base64,{b64}">Download CSV File</a><br /> (This is a raw file. You need to give it the ending .csv, the easiest way is to right-click the link and then click Save as or Save link as, depending on your browser.)'
        st.markdown(href, unsafe_allow_html=True)  # TODO: check if href can be opened in separate tab?


def dataCleaner() -> None: # TODO: Should not be neccessary. Should be done on handler construction.
    state.currentData = state.currentData.replace('None', np.nan)
    itemsPrev = len(state.currentData.index)
    newDF = state.currentData.dropna(axis=0, how='all')
    itemsAfter = len(newDF.index)
    diff = itemsPrev - itemsAfter
    newDF = newDF.drop_duplicates(subset='shelfmark').reset_index(drop=True)
    itemsAfter = len(newDF.index)
    diff1 = itemsPrev - itemsAfter
    st.write(f"Started out with {itemsPrev}, left with {itemsAfter}. Found {diff} NaN values. Found and removed {diff1} duplicates.")
    st.write(newDF)
    if st.button("Keep cleaned data"):
        state.currentData = newDF


def static_reports() -> None:
    '''Page for expensive reports. As of yet only contains one item. Can be expanded later'''
    st.text("Currently not available")
    # reports = {"Dating of all MSs": "all_MS_datings"}  # QUESTION: function not defined
    # selection = st.sidebar.radio("Select report to display", list(reports.keys()), index=0)
    # selected = reports[selection]
    # eval(selected + "()")


def browse_data() -> None:
    handler: DataHandler = state.data_handler
    st.title("Currently Loaded Dataset")

    # Manuscripts
    mss = handler.manuscripts
    st.header("Manuscripts")
    st.write(f"Currently loaded data: Dataframe with {len(mss.index)} entries, {len(mss.columns)} columns each.")
    st.write("Each manuscript can have entries in multiple languages (English, Icelandic, Danish)")
    st.write(f"The present {len(mss.index)} entries correspond to {mss['id'].unique().size} unique manuscripts, \
             stored in {mss['collection'].unique().size} collections.")
    st.write("Head and tail of the dataset:")
    st.dataframe(mss.head().append(mss.tail()))

    # Texts
    txt = handler.texts
    st.header("Texts")
    st.write("Not yet implemented")
    st.dataframe(txt.head())

    # Persons
    pers = handler.persons
    st.header("Persons")
    st.write("Not yet implemented")
    st.dataframe(pers.head())

    # Subcorpora
    subs = handler.subcorpora
    st.header("Sub-Corpora")
    st.write("Not yet implemented")
    st.write(subs)


def help() -> None:
    st.title("How to use this tool")
    with open('CITAVI-README.md', 'r') as citread:
        helpme = markdown.markdown(citread.read())
    st.markdown(helpme, unsafe_allow_html=True)


# Menu Functions
# --------------


def full_menu() -> None:
    '''This is basically the main() and will load and display the full menu, which in turn calls
    all the other functions containing sub pages.
    '''
    handler = state.data_handler
    if handler:
        MenuOptions = {"Home": mainPage,
                       "Browse Data": browse_data,
                       "Search Functions": search_page,
                       "Reports": static_reports,
                       "Advanced Settings": adv_options,
                       "Help": help}
        selection = st.sidebar.selectbox("Menu", list(MenuOptions.keys()))
        selected_function = MenuOptions[selection]
        selected_function()
    else:
        get_handler()


# TODO: move logger to session state, so that it doesn't multi-log

# Run
# ----
if __name__ == '__main__':
    session_state: sessionState.SessionState = sessionState.get(state=StateHandler())  # type: ignore
    state: StateHandler = session_state.state  # type: ignore
    full_menu()
