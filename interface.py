from typing import Any, Optional
import numpy as np
import streamlit as st
import pandas as pd
from datetime import datetime
import markdown
from util import sessionState, tamer
from util import utils
from util.constants import IMAGE_HOME
from util.groups import Group, GroupType
from util.stateHandler import StateHandler, Step
from util.utils import SearchOptions, Settings, GitUtil
from util.datahandler import DataHandler
from gui.guiUtils import Texts
from gui import pages


state: StateHandler
dataHandler: DataHandler
log = utils.get_logger(__name__)
settings = Settings.get_settings()


def get_handler() -> None:
    if DataHandler.is_cached():
        with st.spinner('Grabbing data handler...'):
            rebuild_handler()
    else:
        st.sidebar.text("No data at hand. Needs loading first.")
        adv_options(None)


def rebuild_handler(xmls: Optional[pd.DataFrame] = None, contents: Optional[pd.DataFrame] = None) -> None:
    st.write(f'Start: {datetime.now()}')
    state.data_handler = DataHandler.get_handler(xmls=xmls, contents=contents)
    st.write(f'Finished: {datetime.now()}')
    st.experimental_rerun()
    # full_menu()


# Functions which create sub pages
# --------------------------------------


def mainPage(a: Any) -> None:
    '''Landing page'''
    st.title("Welcome to Sammlung Toole")
    st.write("The Menu on the left has all the options")
    st.image(IMAGE_HOME)


def adv_options(a: Any) -> None:
    '''Shows the advanced options menu'''
    # LATER: At some point we should consider changing crawling into a background task
    st.title("Advanced Options Menu")
    st.write("Careful! Some of these options can take a long time to complete! Like, a loooong time!")
    st.warning("There will be no confirmation on any of these! Clicking any of the option without thinking first is baaad juju!")

    if st.button("Wipe cache"):
        tamer._wipe_cache()
        st.success("Cache is wiped entirely. Please reload the data handler.")
    if st.button("Reload Data Handler"):
        with st.spinner("This may take a while..."):
            state.data_handler = DataHandler.get_handler()
        st.experimental_rerun()


def search_page(a: Any) -> None:
    st.header('Search Page')
    opts = {
        'How To': pages.search.how_to,
        'Handrit URLs': handrit_urls,
        'Search Manuscripts by related People': pages.search.manuscripts_by_persons,
        'Search People by related Manuscripts': pages.search.persons_by_manuscripts,
        'Search Manuscripts by Text': pages.search.manuscripts_by_texts,
        'Search Texts contained by Manuscripts': pages.search.text_by_manuscripts,
    }
    st.sidebar.write("---")
    choice = st.sidebar.radio('What would you like to search?', options=opts.keys())
    fn = opts[choice]
    fn(state)


def handrit_urls(_: Any) -> None:
    '''Workbench. Proper doc to follow soon.'''
    st.title("Result Workflow Builder")
    if state.handrit_step == Step.Handrit_URL.Preprocessing:
        st.header("Preprocessing")
        st.markdown(Texts.SearchPage.instructions)  # XXX: markdown not working here?
        state.currentURLs_str = st.text_area("Input handrit search or browse URL(s) here", help="If multiple URLs, put one URL per line.")

        # state.resultMode = st.radio("Select the type of information you want to extract", ['Contents', 'Metadata'], index=0)
        # state.joinMode = st.radio("Show only shared or all MSs?", ['Shared', 'All'], index=1)
        if st.button("Run"):
            state.didRun = 'Started, dnf.'
            state.handrit_step = Step.Handrit_URL.Processing
            # This block handles data delivery

            if state.currentURLs_str:
                s_urls = [url.strip() for url in state.currentURLs_str.split(',')]
                url_list, state.currentData = state.data_handler.get_ms_urls_from_search_or_browse_urls(
                    urls=s_urls, sharedMode=(state.joinMode == False))
                st.write("Processed Manuscript URLs:")
                st.write(url_list)  # TODO: give indication which strings are being watched, add "clear" button
                state.currentURL_list += url_list
                st.write("Overall MS URLs:")
                st.write(state.currentURL_list)  # TODO: Required?

            if not state.currentData.empty:
                state.didRun = 'OK'
        if state.didRun == 'OK':
            st.header('Results')
            st.write(state.currentData)
    if state.didRun == 'OK':
        if st.button("Go to postprocessing"):
            state.handrit_step = Step.Handrit_URL.Postprocessing
            state.didRun = None
            st.experimental_rerun()
    if state.handrit_step == Step.Handrit_URL.Postprocessing:
        postprocessing()
        if st.button("Go back to preprocessing"):
            state.handrit_step = Step.Handrit_URL.Preprocessing
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
        st.download_button(label="Download", data=csv, file_name="citavi-export.csv")
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
    foundList = state.currentData['shelfmark'].to_list()
    state.CitaviSelect = st.multiselect(label="Select which MSs you want to export to Citavi", options=foundList,
                                        help="This will export your selected references as a CSV file for Citavi.")
    if st.button('Export'):
        state.currentCitaviData = state.currentData.loc(axis=0)[state.currentData['shelfmark'].isin(state.CitaviSelect)]
        state.currentCitaviData = state.currentCitaviData[["id", "creator", "shorttitle", "description", "date", "origin",  "settlement", "repository", "shelfmark"]]
        st.write(state.currentCitaviData)
        csv = state.currentCitaviData.to_csv(sep='\t', encoding='utf-8', index=False)
        st.download_button(label="Download Citavi file", data=csv, file_name="citavi-export.csv", help="There no longer is a bug. It is now safe to just click Download.")


def dataCleaner() -> None:  # TODO: Should not be neccessary. Should be done on handler construction.
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


def static_reports(a: Any) -> None:
    '''Page for expensive reports. As of yet only contains one item. Can be expanded later'''
    st.text("Currently not available")
    # reports = {"Dating of all MSs": "all_MS_datings"}  # QUESTION: function not defined
    # selection = st.sidebar.radio("Select report to display", list(reports.keys()), index=0)
    # selected = reports[selection]
    # eval(selected + "()")


def browse_data(a: Any) -> None:
    handler: DataHandler = state.data_handler
    st.title("Currently Loaded Dataset")

    # Manuscripts
    mss = handler.manuscripts
    st.header("Manuscripts")
    st.write(f"Currently loaded data: Dataframe with {len(mss.index)} entries, {len(mss.columns)} columns each.")
    st.write("Each manuscript can have entries in multiple languages (English, Icelandic, Danish)")
    st.write(f"The present {len(mss.index)} entries correspond to {mss['id'].unique().size} unique manuscripts, \
             stored in {mss['repository'].unique().size} collections.")
    st.write("Head and tail of the dataset:")
    st.dataframe(mss.head().append(mss.tail()))
    if st.button("Show all manuscripts"):
        st.dataframe(mss)

    # Texts
    txt = handler.text_matrix
    st.header("Texts")
    st.write(f'Found {len(txt.columns)} texts.')
    # st.dataframe(txt.head())
    if st.button("List all texts"):
        st.write(txt.columns)
    if st.button("Show text counts"):
        counts = txt.apply(
            lambda x: pd.Series({"count": x[x == True].count()})).transpose().sort_values(
            by=['count'],
            ascending=False).reset_index().rename(
            columns={"index": "text"})
        st.write(counts)

    # Persons
    pers = handler.person_names
    st.header("Persons")
    st.write(f'{len(pers.keys())} people loaded.')
    if st.button("show all"):
        st.write(list(pers.values()))
    pers_matrix = handler.person_matrix
    st.write(f'Built a person-text-matrix of shape: {pers_matrix.shape}')


def help(a: Any) -> None:
    st.markdown(Texts.HowToPage.info)
    if st.button("Show detailed help on Citavi import/export"):
        with open('docs/CITAVI-README.md', 'r') as citread:
            helpme = markdown.markdown(citread.read())
        st.markdown(helpme, unsafe_allow_html=True)


# Menu Functions
# --------------


def full_menu() -> None:
    '''This is basically the main() and will load and display the full menu, which in turn calls
    all the other functions containing sub pages.
    '''
    MenuOptions = {"Home": mainPage,
                   "Browse Data": browse_data,
                   "Groups": pages.groups.browse_groups,
                   "Search Functions": search_page,
                   "Reports": static_reports,
                   "Advanced Settings": adv_options,
                   "Help": help}
    selection = st.sidebar.selectbox("Menu", list(MenuOptions.keys()), on_change=state.steps.reset)
    selected_function = MenuOptions[selection]
    selected_function(state)


def warn_if_handler_not_up_to_date() -> None:
    """ Show a warning, if handler is not up to date with regard to the handrit git submodule """
    if not GitUtil.is_up_to_date():
        delta_t = GitUtil.get_time_difference()
        link = GitUtil.get_comparison_link()
        st.info("Warning: Your data may not be up to date.")
        st.write(f"Time Difference between head and your data: {delta_t}")
        st.markdown(f"See data changes [here]({link}).")
        st.write("Please consider wiping the cache and rebuilding the handler.")
        st.markdown("-----")


if __name__ == '__main__':
    session_state: sessionState.SessionState = sessionState.get(state=StateHandler())  # type: ignore
    state = session_state.state  # type: ignore
    dataHandler = state.data_handler
    warn_if_handler_not_up_to_date()
    if not dataHandler:
        get_handler()
    full_menu()
