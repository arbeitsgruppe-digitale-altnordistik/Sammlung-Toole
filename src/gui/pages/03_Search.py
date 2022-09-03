from typing import Callable

import streamlit as st
from src.gui.gui_utils import get_handler, get_log, get_state
from src.lib import metadatahandler
from src.lib.groups import Group, GroupType
from src.lib.stateHandler import Step
from src.lib.utils import SearchOptions

log = get_log()
state = get_state()
handler = get_handler()


def search_page() -> None:
    st.header('Search Page')
    opts: dict[str, Callable[[], None]] = {
        'How To': how_to,
        'Search Manuscripts by related People': manuscripts_by_persons,
        'Search People by related Manuscripts': persons_by_manuscripts,
        'Search Manuscripts by Text': manuscripts_by_texts,
        'Search Texts contained by Manuscripts': text_by_manuscripts,
    }
    choice = st.sidebar.radio('What would you like to search?', options=list(opts.keys()))
    fn = opts[choice]
    fn()


# How To Search
# =============

def how_to() -> None:
    """How-To page of the search page"""
    st.markdown("""
                # How To Search

                Please select one of the search options on the left in the navigation bar.

                The following search options are available:

                - Manuscript by Person:
                  Select one/multiple persons form the Handrit.is authority file.  
                  The tool will find all manuscripts related to one/all of the selected people.


                - Person by Manuscript:
                  Select one/multiple manuscripts form the Handrit.is collection.  
                  The tool will find all people related to one/all of the selected manuscripts.


                - Manuscript by Text:
                  Select one/multiple texts mentioned in the Handrit.is collections.  
                  The tool will find all manuscripts related to one/all of the selected texts.


                - Text by Manuscript:
                  Select one/multiple manuscripts form the Handrit.is collection.  
                  The tool will find all texts occuring in one/all of the selected manuscripts.
                """)


# Search for manuscripts by person
# ================================


def manuscripts_by_persons() -> None:
    """Search Page: Search for manuscripts by persons related to the manuscripts.

    Args:
        state (StateHandler): The current session state.
    """
    if state.steps.search_mss_by_persons == Step.MS_by_Pers.Search_person:
        __search_mss_by_person_step_search()
        print("Initial search step")
    else:
        print("Got some results, trying to display them:")
        __search_mss_by_person_step_save_results()


def __search_mss_by_person_step_search() -> None:
    """
    Step 1 of this search: Select person(s).
    """
    with st.form("search_ms_by_person"):
        st.subheader("Select Person(s)")
        modes = {'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
                 'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE}
        mode_selection = st.radio('Search mode', list(modes.keys()), 1)
        mode = modes[mode_selection]
        ppl = st.multiselect('Select Person', list(handler.person_names.keys()), format_func=lambda x: f"{handler.person_names[x]} ({x})")
        if st.form_submit_button("Search Manuscripts"):
            log.debug(f'Search Mode: {mode}')
            log.debug(f'selected people: {ppl}')
            with st.spinner('Searching...'):
                res = handler.search_manuscripts_related_to_persons(ppl, mode)
                state.searchState.ms_by_pers.mss = res
                state.searchState.ms_by_pers.ppl = ppl
                state.searchState.ms_by_pers.mode = mode
            state.steps.search_mss_by_persons = Step.MS_by_Pers.Store_Results
            st.experimental_rerun()


def __search_mss_by_person_step_save_results() -> None:
    """
    Step 2 of this search: Do something with the result.
    """
    results = state.searchState.ms_by_pers.mss
    ppl = state.searchState.ms_by_pers.ppl
    mode = state.searchState.ms_by_pers.mode
    st.subheader("Person(s) selected")
    query = f' {mode.value} '.join([f"{handler.get_person_name(x)} ({x})" for x in ppl])
    st.write(f"Searched for '{query}', found {len(results)} manuscripts")
    if st.button("Back"):
        state.steps.search_mss_by_persons = Step.MS_by_Pers.Search_person
        st.experimental_rerun()
    with st.expander('view results as list', False):
        st.write(results)
    metadatahandler.process_ms_results(handler, results)
    with st.expander("Save results as group", False):
        with st.form("save_group"):
            name = st.text_input('Group Name', f'Search results for <{ppl}>')
            if st.form_submit_button("Save"):
                grp = Group(GroupType.ManuscriptGroup, name, set(results))
                # log.debug(f"Should be saving group: {grp}")
                handler.groups.set(grp)
                state.steps.search_mss_by_persons = Step.MS_by_Pers.Search_person
                st.experimental_rerun()
    if handler.groups.manuscript_groups:
        with st.expander("Add results to existing group", False):
            with st.form("add_to_group"):
                previous_name = st.radio("Select a group", handler.groups.get_names(GroupType.ManuscriptGroup))
                modes = {
                    'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE,
                    'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
                }
                mode_selection = st.radio('Search mode', list(modes.keys()))
                mode = modes[mode_selection]
                name = st.text_input('Group Name', f'Search results for <{ppl} AND/OR ([PREVIOUS_QUERY])>')
                if st.form_submit_button("Save"):
                    previous_group = handler.groups.get_group_by_name(previous_name, GroupType.ManuscriptGroup)
                    if previous_group:
                        previous_query = previous_name.removeprefix("Search results for <").removesuffix(">")
                        new_name = f'Search results for <{ppl} {mode.value} ({previous_query})>'
                        if mode == SearchOptions.CONTAINS_ALL:
                            new_items = previous_group.items.intersection(set(results))
                        else:
                            new_items = previous_group.items.union(set(results))
                        new_group = Group(previous_group.group_type,  new_name, new_items)
                        handler.groups.set(new_group)
                        state.steps.search_mss_by_persons = Step.MS_by_Pers.Search_person
                        st.experimental_rerun()


# Search for people by manuscript
# ===============================

def persons_by_manuscripts() -> None:
    """Search Page: Search for persons by manuscripts related to the person.

    Args:
        state (StateHandler): The current session state.
    """
    if state.steps.search_ppl_by_mss == Step.Pers_by_Ms.Search_Ms:
        __search_person_by_mss_step_search()
    else:
        __search_person_by_mss_step_save_results()


def __search_person_by_mss_step_search() -> None:
    """
    Step 1 of this search: Select manuscript(s).
    """
    with st.form("search_person_by_ms"):
        st.subheader("Select Manuscript(s)")
        modes = {'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
                 'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE}
        mode_selection = st.radio('Search mode', list(modes.keys()), index=1)
        mode = modes[mode_selection]
        mss = st.multiselect('Select Manuscript', list(handler.manuscripts.keys()), format_func=lambda x: f"{' / '.join(handler.manuscripts[x])} ({x})")
        if st.form_submit_button("Search People"):
            log.debug(f'Search Mode: {mode}')
            log.debug(f'selected manuscripts: {mss}')
            with st.spinner('Searching...'):
                res = handler.search_persons_related_to_manuscripts(mss, mode)
                state.searchState.pers_by_ms.ppl = res
                state.searchState.pers_by_ms.mss = mss
                state.searchState.pers_by_ms.mode = mode
            state.steps.search_ppl_by_mss = Step.Pers_by_Ms.Store_Results
            st.experimental_rerun()


def __search_person_by_mss_step_save_results() -> None:
    """
    Step 2 of this search: Do something with the result.
    """
    results = state.searchState.pers_by_ms.ppl
    mss = state.searchState.pers_by_ms.mss
    mode = state.searchState.pers_by_ms.mode
    st.subheader("Manuscript(s) selected")
    query = f' {mode.value} '.join([f"({x})" for x in mss])
    st.write(f"Searched for '{query}', found {len(results)} {'person' if len(results) == 1 else 'people'}")
    if st.button("Back"):
        state.steps.search_ppl_by_mss = Step.Pers_by_Ms.Search_Ms
        st.experimental_rerun()
    with st.expander('view results as list', False):
        resList = [handler.person_names[x] for x in results]
        st.write(resList)
    with st.expander("Save results as group", False):
        with st.form("save_group"):
            name = st.text_input('Group Name', f'Search results for <{mss}>')
            if st.form_submit_button("Save"):
                grp = Group(GroupType.PersonGroup, name, set(results))
                # log.debug(f"Should be saving group: {grp}")
                handler.groups.set(grp)
                state.steps.search_ppl_by_mss = Step.Pers_by_Ms.Search_Ms
                st.experimental_rerun()
    if handler.groups.person_groups:
        with st.expander("Add results to existing group", False):
            with st.form("add_to_group"):
                previous_name = st.radio("Select a group", handler.groups.get_names(GroupType.PersonGroup))
                modes = {
                    'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE,
                    'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
                }
                mode_selection = st.radio('Search mode', list(modes.keys()))
                mode = modes[mode_selection]
                name = st.text_input('Group Name', f'Search results for <{mss} AND/OR ([PREVIOUS_QUERY])>')
                if st.form_submit_button("Save"):
                    previous_group = handler.groups.get_group_by_name(previous_name, GroupType.PersonGroup)
                    if previous_group:
                        previous_query = previous_name.removeprefix("Search results for<").removesuffix(">")
                        new_name = f'Search results for <{mss} {mode.value} ({previous_query})>'
                        if mode == SearchOptions.CONTAINS_ALL:
                            new_items = previous_group.items.intersection(set(results))
                        else:
                            new_items = previous_group.items.union(set(results))
                        new_group = Group(previous_group.group_type,  new_name, new_items)
                        handler.groups.set(new_group)
                        state.steps.search_ppl_by_mss = Step.Pers_by_Ms.Search_Ms
                        st.experimental_rerun()
    # TODO: What now?


# Search for manuscripts by text
# ==============================

def manuscripts_by_texts() -> None:
    """Search Page: Search for manuscripts by texts within the manuscripts.

    Args:
        state (StateHandler): The current session state.
    """
    if state.steps.search_mss_by_txt == Step.MS_by_Txt.Search_Txt:
        __search_mss_by_text_step_search()
    else:
        __search_mss_by_text_step_save_results()


def __search_mss_by_text_step_search() -> None:
    """
    Step 1 of this search: Select text(s).
    """
    with st.form("search_ms_by_text"):
        st.subheader("Select Text(s)")
        modes = {'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
                 'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE}
        mode_selection = st.radio('Search mode', list(modes.keys()), 1)
        mode = modes[mode_selection]
        txt = st.multiselect('Select Text', handler.texts)
        # LATER: find format function to make it pretty
        if st.form_submit_button("Search Manuscripts"):
            log.debug(f'Search Mode: {mode}')
            log.debug(f'selected people: {txt}')
            with st.spinner('Searching...'):
                res = handler.search_manuscripts_containing_texts(txt, mode)
                state.searchState.ms_by_txt.mss = res
                state.searchState.ms_by_txt.txt = txt
                state.searchState.ms_by_txt.mode = mode
            state.steps.search_mss_by_txt = Step.MS_by_Txt.Store_Results
            st.experimental_rerun()


def __search_mss_by_text_step_save_results() -> None:
    """
    Step 2 of this search: Do something with the result.
    """
    results = state.searchState.ms_by_txt.mss
    txt = state.searchState.ms_by_txt.txt
    mode = state.searchState.ms_by_txt.mode
    st.subheader("Text(s) selected")
    query = f' {mode.value} '.join(txt)
    st.write(f"Searched for '{query}', found {len(results)} manuscripts")
    if st.button("Back"):
        state.steps.search_mss_by_txt = Step.MS_by_Txt.Search_Txt
        st.experimental_rerun()
    with st.expander('view results as list', False):
        st.write([handler.manuscripts[x] for x in results])
    with st.expander("Save results as group", False):
        with st.form("save_group"):
            name = st.text_input('Group Name', f'Search results for <{txt}>')
            if st.form_submit_button("Save"):
                grp = Group(GroupType.ManuscriptGroup, name, set(results))
                # log.debug(f"Should be saving group: {grp}")
                handler.groups.set(grp)
                state.steps.search_mss_by_txt = Step.MS_by_Txt.Search_Txt
                st.experimental_rerun()
    metadatahandler.process_ms_results(handler, results)
    if handler.groups.manuscript_groups:
        with st.expander("Add results to existing group", False):
            with st.form("add_to_group"):
                previous_name = st.radio("Select a group", handler.groups.get_names(GroupType.ManuscriptGroup))
                modes = {
                    'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE,
                    'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
                }
                mode_selection = st.radio('Search mode', list(modes.keys()))
                mode = modes[mode_selection]
                name = st.text_input('Group Name', f'Search results for <{txt} AND/OR ([PREVIOUS_QUERY])>')
                if st.form_submit_button("Save"):
                    previous_group = handler.groups.get_group_by_name(previous_name, GroupType.ManuscriptGroup)
                    if previous_group:
                        previous_query = previous_name.removeprefix("Search results for <").removesuffix(">")
                        new_name = f'Search results for <{txt} {mode.value} ({previous_query})>'
                        if mode == SearchOptions.CONTAINS_ALL:
                            new_items = previous_group.items.intersection(set(results))
                        else:
                            new_items = previous_group.items.union(set(results))
                        new_group = Group(previous_group.group_type,  new_name, new_items)
                        handler.groups.set(new_group)
                        state.steps.search_mss_by_txt = Step.MS_by_Txt.Search_Txt
                        st.experimental_rerun()


# Search for texts by manuscript
# ==============================

def text_by_manuscripts() -> None:
    """Search Page: Search for texts by manuscripts containing the text.

    Args:
        state (StateHandler): The current session state.
    """
    if state.steps.search_txt_by_mss == Step.Txt_by_Ms.Search_Ms:
        __search_text_by_mss_step_search()
    else:
        __search_text_by_mss_step_save_results()


def __search_text_by_mss_step_search() -> None:
    """
    Step 1 of this search: Select manuscript(s).
    """
    with st.form("search_text_by_ms"):
        st.subheader("Select Manuscript(s)")
        modes = {'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
                 'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE}
        mode_selection = st.radio('Search mode', list(modes.keys()), 1)
        mode = modes[mode_selection]
        mss = st.multiselect('Select Manuscript', list(handler.manuscripts.keys()), format_func=lambda x: f"{' / '.join(handler.manuscripts[x])} ({x})")
        if st.form_submit_button("Search Texts"):
            log.debug(f'Search Mode: {mode}')
            log.debug(f'selected manuscripts: {mss}')
            with st.spinner('Searching...'):
                res = handler.search_texts_contained_by_manuscripts(mss, mode)
                state.searchState.txt_by_ms.txt = res
                state.searchState.txt_by_ms.mss = mss
                state.searchState.txt_by_ms.mode = mode
            state.steps.search_txt_by_mss = Step.Txt_by_Ms.Store_Results
            st.experimental_rerun()


def __search_text_by_mss_step_save_results() -> None:
    """
    Step 2 of this search: Do something with the result.
    """
    results = state.searchState.txt_by_ms.txt
    if not results:
        state.steps.search_txt_by_mss = Step.Txt_by_Ms.Search_Ms
        st.experimental_rerun()
    mss = state.searchState.txt_by_ms.mss
    mode = state.searchState.txt_by_ms.mode
    st.subheader("Manuscript(s) selected")
    query = f' {mode.value} '.join([f"({x})" for x in mss])
    st.write(f"Searched for '{query}', found {len(results)} {'text' if len(results) == 1 else 'texts'}")
    if st.button("Back"):
        state.steps.search_txt_by_mss = Step.Txt_by_Ms.Search_Ms
        st.experimental_rerun()
    with st.expander('view results as list', False):
        st.write(results)
    with st.expander("Save results as group", False):
        with st.form("save_group"):
            name = st.text_input('Group Name', f'Search results for manuscript search <{mss}>')
            if st.form_submit_button("Save"):
                grp = Group(GroupType.TextGroup, name, set(results))
                # log.debug(f"Should be saving group: {grp}")
                handler.groups.set(grp)
                state.steps.search_txt_by_mss = Step.Txt_by_Ms.Search_Ms
                st.experimental_rerun()
    if handler.groups.text_groups:
        with st.expander("Add results to existing group", False):
            with st.form("add_to_group"):
                previous_name = st.radio("Select a group", handler.groups.get_names(GroupType.TextGroup))
                modes = {
                    'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE,
                    'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
                }
                mode_selection = st.radio('Search mode', list(modes.keys()))
                mode = modes[mode_selection]
                name = st.text_input('Group Name', f'Search results for <{mss} AND/OR ([PREVIOUS_QUERY])>')
                if st.form_submit_button("Save"):
                    previous_group = handler.groups.get_group_by_name(previous_name, GroupType.PersonGroup)
                    if previous_group:
                        previous_query = previous_name.removeprefix("Search results for <").removesuffix(">")
                        new_name = f'Search results for <{mss} {mode.value} ({previous_query})>'
                        if mode == SearchOptions.CONTAINS_ALL:
                            new_items = previous_group.items.intersection(set(results))
                        else:
                            new_items = previous_group.items.union(set(results))
                        new_group = Group(previous_group.group_type,  new_name, new_items)
                        handler.groups.set(new_group)
                        state.steps.search_txt_by_mss = Step.Txt_by_Ms.Search_Ms
                        st.experimental_rerun()
    # TODO: visualization/citavi-export of result


search_page()
