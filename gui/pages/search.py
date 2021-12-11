
from logging import Logger
import streamlit as st
from util import utils
from util.datahandler import DataHandler
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
    st.markdown("""
                ### How To Search
                
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
    if state.steps.search_mss_by_persons == Step.MS_by_Pers.Search_person:
        __search_mss_by_person_step_search(state)
    else:
        __search_mss_by_person_step_save_results(state)


def __search_mss_by_person_step_search(state: StateHandler) -> None:
    handler = state.data_handler
    with st.form("search_ms_by_person"):
        st.subheader("Select Person(s)")
        persons = list(handler.person_matrix.columns)
        modes = {'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
                 'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE}
        mode_selection = st.radio('Search mode', modes.keys())
        mode = modes[mode_selection]
        ppl = st.multiselect('Search Person', persons, format_func=lambda x: f"{handler.get_person_name(x)} ({x})")
        if st.form_submit_button("Search Manuscripts"):
            log.debug(f'Search Mode: {mode}')
            log.debug(f'selected people: {ppl}')
            with st.spinner('Searching...'):
                res = handler.search_manuscripts_related_to_persons(ppl, mode)
                state.search_ms_by_person_result_mss = res
                state.search_ms_by_person_result_ppl = ppl
                state.search_ms_by_person_result_mode = mode
            state.steps.search_mss_by_persons = Step.MS_by_Pers.Store_Results
            st.experimental_rerun()


def __search_mss_by_person_step_save_results(state: StateHandler) -> None:
    handler = state.data_handler
    results = state.search_ms_by_person_result_mss
    if not results:
        state.steps.search_mss_by_persons = Step.MS_by_Pers.Search_person
        st.experimental_rerun()
    ppl = state.search_ms_by_person_result_ppl
    mode = state.search_ms_by_person_result_mode
    st.subheader("Person(s) selected")
    query = f' {mode.value} '.join([f"{handler.get_person_name(x)} ({x})" for x in ppl])
    st.write(f"Searched for '{query}', found {len(results)} manuscripts")
    if st.button("Back"):
        state.steps.search_mss_by_persons = Step.MS_by_Pers.Search_person
        st.experimental_rerun()
    with st.expander('view results as list', False):
        st.write(results)
    with st.expander("Save results as group", False):
        with st.form("save_group"):
            name = st.text_input('Group Name', f'Search results for person search <{ppl}>')
            if st.form_submit_button("Save"):
                grp = Group(GroupType.ManuscriptGroup, name, set(results))
                log.debug(f"Should be saving group: {grp}")
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
                mode_selection = st.radio('Search mode', modes.keys())
                mode = modes[mode_selection]
                name = st.text_input('Group Name', f'Search results for person search <{ppl} AND/OR ([PREVIOUS_QUERY])>')
                if st.form_submit_button("Save"):
                    previous_group = handler.groups.get_group_by_name(previous_name, GroupType.ManuscriptGroup)
                    if previous_group:
                        previous_query = previous_name.removeprefix("Search results for person search <").removesuffix(">")
                        new_name = f'Search results for person search <{ppl} {mode.value} ({previous_query})>'
                        if mode == SearchOptions.CONTAINS_ALL:
                            new_items = previous_group.items.intersection(set(results))
                        else:
                            new_items = previous_group.items.union(set(results))
                        new_group = Group(previous_group.group_type,  new_name, new_items)
                        handler.groups.set(new_group)
                        state.steps.search_mss_by_persons = Step.MS_by_Pers.Search_person
                        st.experimental_rerun()
    meta = handler.search_manuscript_data(full_ids=results).reset_index(drop=True)  # type: ignore
    st.dataframe(meta)

# endregion


# Search for people by manuscript
# ===============================
# region

def persons_by_manuscripts(state: StateHandler) -> None:
    if state.steps.search_ppl_by_mss == Step.Pers_by_Ms.Search_Ms:
        __search_person_by_mss_step_search(state)
    else:
        __search_person_by_mss_step_save_results(state)


def __search_person_by_mss_step_search(state: StateHandler) -> None:
    handler = state.data_handler
    with st.form("search_person_by_ms"):
        st.subheader("Select Manuscript(s)")
        manuscripts = list(handler.person_matrix.index)
        modes = {'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
                 'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE}
        mode_selection = st.radio('Search mode', modes.keys())
        mode = modes[mode_selection]
        # LATER: come up with a nice format function here (ideally including ms nicknames, sothat one could find "Flateyjarbók" etc.)
        mss = st.multiselect('Select Manuscript', manuscripts)
        if st.form_submit_button("Search People"):
            log.debug(f'Search Mode: {mode}')
            log.debug(f'selected people: {mss}')
            with st.spinner('Searching...'):
                res = handler.search_persons_related_to_manuscripts(mss, mode)
                state.search_person_by_ms_result_ppl = res
                state.search_person_by_ms_result_mss = mss
                state.search_person_by_ms_result_mode = mode
            state.steps.search_ppl_by_mss = Step.Pers_by_Ms.Store_Results
            st.experimental_rerun()


def __search_person_by_mss_step_save_results(state: StateHandler) -> None:
    handler = state.data_handler
    results = state.search_person_by_ms_result_ppl
    if not results:
        state.steps.search_ppl_by_mss = Step.Pers_by_Ms.Search_Ms
        st.experimental_rerun()
    mss = state.search_person_by_ms_result_mss
    mode = state.search_person_by_ms_result_mode
    st.subheader("Manuscript(s) selected")
    query = f' {mode.value} '.join([f"({x})" for x in mss])
    st.write(f"Searched for '{query}', found {len(results)} {'person' if len(results) == 1 else 'people'}")
    if st.button("Back"):
        state.steps.search_ppl_by_mss = Step.Pers_by_Ms.Search_Ms
        st.experimental_rerun()
    with st.expander('view results as list', False):
        st.write(results)
    with st.expander("Save results as group", False):
        with st.form("save_group"):
            name = st.text_input('Group Name', f'Search results for manuscript search <{mss}>')
            if st.form_submit_button("Save"):
                grp = Group(GroupType.PersonGroup, name, set(results))
                log.debug(f"Should be saving group: {grp}")
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
                mode_selection = st.radio('Search mode', modes.keys())
                mode = modes[mode_selection]
                name = st.text_input('Group Name', f'Search results for person search <{mss} AND/OR ([PREVIOUS_QUERY])>')
                if st.form_submit_button("Save"):
                    previous_group = handler.groups.get_group_by_name(previous_name, GroupType.PersonGroup)
                    if previous_group:
                        previous_query = previous_name.removeprefix("Search results for person search <").removesuffix(">")
                        new_name = f'Search results for person search <{mss} {mode.value} ({previous_query})>'
                        if mode == SearchOptions.CONTAINS_ALL:
                            new_items = previous_group.items.intersection(set(results))
                        else:
                            new_items = previous_group.items.union(set(results))
                        new_group = Group(previous_group.group_type,  new_name, new_items)
                        handler.groups.set(new_group)
                        state.steps.search_ppl_by_mss = Step.Pers_by_Ms.Search_Ms
                        st.experimental_rerun()
    st.table([(x, handler.get_person_name(x)) for x in results])

# endregion
