
from logging import Logger

import streamlit as st
from util import metadatahandler, utils
from util.groups import Group, GroupType
from util.stateHandler import StateHandler, Step
from util.utils import SearchOptions


@st.experimental_singleton   # type: ignore
def get_log() -> Logger:
    return utils.get_logger(__name__)


log: Logger = get_log()


# How To Search
# =============
# region

def how_to(_: StateHandler) -> None:
    """How-To page of the search page"""
    st.markdown("""
                # How To Search

                Please select one of the search options on the left in the navigation bar.

                The following search options are available:

                - Handrit URLs:
                  Use one/multiple search- or browse result URL from Handrit.is.
                  The tool will find the manuscripts as retuirned by Handrit.is, and show the according metadata.


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

# endregion


# Search for manuscripts by person
# ================================
# region

def manuscripts_by_persons(state: StateHandler) -> None:
    """Search Page: Search for manuscripts by persons related to the manuscripts.

    Args:
        state (StateHandler): The current session state.
    """
    if state.steps.search_mss_by_persons == Step.MS_by_Pers.Search_person:
        __search_mss_by_person_step_search(state)
        print("Initial search step")
    else:
        print("Got some results, trying to display them:")
        __search_mss_by_person_step_save_results(state)


def __search_mss_by_person_step_search(state: StateHandler) -> None:
    """
    Step 1 of this search: Select person(s).
    """
    handler = state.data_handler
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


def __search_mss_by_person_step_save_results(state: StateHandler) -> None:
    """
    Step 2 of this search: Do something with the result.
    """
    handler = state.data_handler
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
    metadatahandler.process_ms_results(state, results)
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


# endregion


# Search for people by manuscript
# ===============================
# region

def persons_by_manuscripts(state: StateHandler) -> None:
    """Search Page: Search for persons by manuscripts related to the person.

    Args:
        state (StateHandler): The current session state.
    """
    if state.steps.search_ppl_by_mss == Step.Pers_by_Ms.Search_Ms:
        __search_person_by_mss_step_search(state)
    else:
        __search_person_by_mss_step_save_results(state)


def __search_person_by_mss_step_search(state: StateHandler) -> None:
    """
    Step 1 of this search: Select manuscript(s).
    """
    handler = state.data_handler
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


def __search_person_by_mss_step_save_results(state: StateHandler) -> None:
    """
    Step 2 of this search: Do something with the result.
    """
    handler = state.data_handler
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

# endregion


# Search for manuscripts by text
# ==============================
# region

def manuscripts_by_texts(state: StateHandler) -> None:
    """Search Page: Search for manuscripts by texts within the manuscripts.

    Args:
        state (StateHandler): The current session state.
    """
    if state.steps.search_mss_by_txt == Step.MS_by_Txt.Search_Txt:
        __search_mss_by_text_step_search(state)
    else:
        __search_mss_by_text_step_save_results(state)


def __search_mss_by_text_step_search(state: StateHandler) -> None:
    """
    Step 1 of this search: Select text(s).
    """
    handler = state.data_handler
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


def __search_mss_by_text_step_save_results(state: StateHandler) -> None:
    """
    Step 2 of this search: Do something with the result.
    """
    handler = state.data_handler
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
    metadatahandler.process_ms_results(state, results)
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

# endregion


# Search for texts by manuscript
# ==============================
# region

def text_by_manuscripts(state: StateHandler) -> None:
    """Search Page: Search for texts by manuscripts containing the text.

    Args:
        state (StateHandler): The current session state.
    """
    if state.steps.search_txt_by_mss == Step.Txt_by_Ms.Search_Ms:
        __search_text_by_mss_step_search(state)
    else:
        __search_text_by_mss_step_save_results(state)


def __search_text_by_mss_step_search(state: StateHandler) -> None:
    """
    Step 1 of this search: Select manuscript(s).
    """
    handler = state.data_handler
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


def __search_text_by_mss_step_save_results(state: StateHandler) -> None:
    """
    Step 2 of this search: Do something with the result.
    """
    handler = state.data_handler
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
    st.write(results)
    # TODO: visualization/citavi-export of result

# endregion
