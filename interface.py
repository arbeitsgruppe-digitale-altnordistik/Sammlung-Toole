from typing import Container, Optional
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
from util.utils import SearchOptions, Settings
from util.datahandler import DataHandler
from gui.guiUtils import Texts


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
        adv_options()


def rebuild_handler(xmls: Optional[pd.DataFrame] = None, contents: Optional[pd.DataFrame] = None) -> None:
    st.write(f'Start: {datetime.now()}')
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
    st.write("Careful! Some of these options can take a long time to complete! Like, a loooong time!")
    st.warning("There will be no confirmation on any of these! Clicking any of the option without thinking first is baaad juju!")

    if st.button("Wipe cache"):
        tamer._wipe_cache()
        st.success("Cache is wiped entirely. Please reload the data handler.")
    if st.button("Reload Data Handler"):
        with st.spinner("This may take a while..."):
            state.data_handler = DataHandler.get_handler()
        st.experimental_rerun()


def search_page() -> None:
    st.header('Search Page')
    opts = {
        'How To': explain_search_options,
        'Handrit URLs': handrit_urls,
        'Search Manuscripts by related People': search_mss_by_persons,
        'Search People by related Manuscripts': search_ppl_by_manuscripts,
        'Search Manuscripts by Text': search_mss_by_texts,
        'Search Texts contained by Manuscripts': search_text_by_mss,
    }
    choice = st.sidebar.radio('What would you like to search?', options=opts.keys())
    fn = opts[choice]
    fn()


def search_ppl_by_manuscripts() -> None:
    mss_ = list(dataHandler.person_matrix.index)
    _mss = dataHandler.manuscripts[dataHandler.manuscripts['full_id'].isin(mss_)]
    mss = _mss['shelfmark'].tolist()
    with st.expander('View all manuscripts', False):
        st.write(mss)
    modes = {'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
             'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE}
    mode_selection = st.radio('Search mode', modes.keys())
    mode = modes[mode_selection]
    log.debug(f'Search Mode: {mode}')
    msss_ = st.multiselect('Search Manuscript', mss)
    _msss = _mss[_mss['shelfmark'].isin(msss_)]
    msss = list(set(_msss['full_id'].tolist()))
    log.debug(f'selected manuscript(s): {msss}')
    with st.spinner('Searching...'):
        results = dataHandler.search_persons_related_to_manuscripts(msss, mode)
    st.write(f'Found {len(results)} people')
    if results:
        with st.expander('view results', False):
            full_names = {k: dataHandler.get_person_name(k) for k in results}
            st.write(full_names)
    # TODO: should do something with it here (further search, subcorpora, ...)


def search_mss_by_persons() -> None:
    if state.ms_by_pers_step == Step.MS_by_Pers.Search_person:
        st.subheader("Select Person(s)")
        persons = list(dataHandler.person_matrix.columns)
        with st.expander('View all People', False):
            st.write(persons)
        modes = {'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
                 'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE}
        mode_selection = st.radio('Search mode', modes.keys())
        mode = modes[mode_selection]
        log.debug(f'Search Mode: {mode}')
        ppl = st.multiselect('Search Person', persons)
        log.debug(f'selected people: {ppl}')
        with st.expander('Show full names'):
            fullnames = {k: dataHandler.get_person_name(k) for k in ppl}
            st.write(fullnames)
        with st.spinner('Searching...'):
            res = dataHandler.search_manuscripts_related_to_persons(ppl, mode)
            state.search_ms_by_person_result_mss = res
            state.search_ms_by_person_result_ppl = ppl
        st.write(f'Found {len(res)} manuscripts')
        if res and st.button("OK"):
            state.ms_by_pers_step = Step.MS_by_Pers.blah
            st.experimental_rerun()
    else:
        st.subheader("Person(s) selected")
        if st.button("Back"):
            state.ms_by_pers_step = Step.MS_by_Pers.Search_person
            st.experimental_rerun()
        results = state.search_ms_by_person_result_mss
        ppl = state.search_ms_by_person_result_ppl
        if not results:
            state.ms_by_pers_step = Step.MS_by_Pers.Search_person
            return
        with st.expander('view results as list', False):
            st.write(results)
        with st.expander("Save results as group", False):
            with st.form("save_group"):
                name = st.text_input('Group Name', f'Search results for person search {ppl}')
                if st.form_submit_button("Save"):
                    gr = Group(GroupType.PersonGroup, name, set(results))
                    dataHandler.groups.append(gr)
        with st.expander("Add results to existing group", False):
            with st.form("add_to_group"):
                gr = st.radio("Select a group", ['...', 'xxx'])
                copy = st.checkbox("Save as new copy (if not, the group will be overwritten)")
                if st.form_submit_button("Save"):
                    pass
        if st.button('Show metadata for results'):
            with st.spinner('loading metadata...'):
                meta = dataHandler.search_manuscript_data(full_ids=results).reset_index(drop=True)  # type:ignore
            st.write(meta)
    # TODO: should do something with it here (export, subcorpora, ...)


def search_text_by_mss() -> None:
    mss = list(set(dataHandler.manuscripts['shelfmark']))
    with st.expander('View all Manuscripts', False):
        st.write(mss)
    modes = {'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
             'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE}
    mode_selection = st.radio('Search mode', modes.keys())
    mode = modes[mode_selection]
    log.debug(f'Search Mode: {mode}')
    msss = st.multiselect('Search Manuscripts', mss)
    log.debug(f'selected manuscripts: {msss}')
    with st.spinner('Searching...'):
        results = dataHandler.search_texts_contained_by_manuscripts(msss, mode)
    st.write(f'Found {len(results)} texts')
    if results:
        with st.expander('view results', False):
            preview = st.container()
            preview.write("List of texts found:")
            count = 1
            for i in results:
                preview.write(f"{count}: {i}")
                count += 1
    # TODO: do something with it here (further search? subcorpus, ...)


def search_mss_by_texts() -> None:
    texts = list(dataHandler.get_all_texts().columns)
    with st.expander('View all Texts', False):
        st.write(texts)
    modes = {'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
             'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE}
    mode_selection = st.radio('Search mode', modes.keys())
    mode = modes[mode_selection]
    log.debug(f'Search Mode: {mode}')
    txts = st.multiselect('Search Texts', texts)
    log.debug(f'selected texts: {txts}')
    with st.spinner('Searching...'):
        results = dataHandler.search_manuscripts_containing_texts(txts, mode)
    st.write(f'Found {len(results)} manuscripts')
    if results:
        with st.expander('view results', False):
            preview = st.container()
            preview.write("List of manuscripts found:")
            count = 1
            for i in results:
                preview.write(f"{count}: {i}")
                count += 1
    if st.button('Get metadata for results'):
        with st.spinner('loading metadata...'):
            meta = dataHandler.search_manuscript_data(shelfmarks=results).reset_index(drop=True)  # type:ignore
        st.write(meta)
    # TODO: should do something with it here (export, subcorpora, ...)


def explain_search_options() -> None:
    st.write('Please choose a search option.')
    # TODO: more explanation


def handrit_urls() -> None:
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
                    urls=s_urls, sharedMode=(state.joinMode == False))  # type: ignore  # LATER: find solution for this type error
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
            state.didRun = None  # type: ignore  # LATER: find solution for this type error
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


def browse_groups() -> None:
    handler: DataHandler = state.data_handler
    groups = handler.groups
    st.title("Groups")

    st.header("Groups")
    st.write("Not yet implemented")
    st.write(groups)


def help() -> None:
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
                   "Groups": browse_groups,
                   "Search Functions": search_page,
                   "Reports": static_reports,
                   "Advanced Settings": adv_options,
                   "Help": help}
    selection = st.sidebar.selectbox("Menu", list(MenuOptions.keys()))
    selected_function = MenuOptions[selection]
    selected_function()


# TODO: move logger to session state, so that it doesn't multi-log

# Run
# ----
if __name__ == '__main__':
    session_state: sessionState.SessionState = sessionState.get(state=StateHandler())  # type: ignore
    state = session_state.state  # type: ignore
    dataHandler = state.data_handler
    if not dataHandler:
        get_handler()
    full_menu()
