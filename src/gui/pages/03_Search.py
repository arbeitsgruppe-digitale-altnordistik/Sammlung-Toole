from typing import Any, Callable

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
    else:
        __search_mss_by_person_step_save_results()


def __search_mss_by_person_step_search() -> None:
    """
    Step 1 of this search: Select person(s).
    """
    __search_step_1(
        what_sg="Person",
        what_pl="People",
        selection_keys=list(handler.person_names.keys()),
        search_func=handler.search_manuscripts_related_to_persons,
        state_func=state.store_ms_by_person_search_state,
        format_func=lambda x: f"{handler.person_names[x]} ({x})",
    )


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
    __show_as_list(results)
    def next_step() -> None: state.steps.search_mss_by_persons = Step.MS_by_Pers.Search_person
    __save_group(
        ids=results,
        searchterms=ppl,
        grouptype=GroupType.ManuscriptGroup,
        step_func=next_step
    )
    __add_to_group(
        ids=results,
        searchterms=ppl,
        groups=handler.get_ms_groups(),
        step_func=next_step
    )
    # LATER: metadatahandler is currently a mix of util and UI... should probably be divided and put in the right place
    metadatahandler.process_ms_results(handler, results)


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
    __search_step_1(
        what_sg="Manuscript",
        what_pl="Manuscripts",
        selection_keys=list(handler.manuscripts.keys()),
        search_func=handler.search_persons_related_to_manuscripts,
        state_func=state.store_ppl_by_ms_search_state,
        format_func=lambda x: f"{' / '.join(handler.manuscripts[x])} ({x})",
    )


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
    __show_as_list([handler.person_names[x] for x in results])
    def next_step() -> None: state.steps.search_ppl_by_mss = Step.Pers_by_Ms.Search_Ms
    __save_group(
        ids=results,
        searchterms=mss,
        grouptype=GroupType.PersonGroup,
        step_func=next_step
    )
    __add_to_group(
        ids=results,
        searchterms=mss,
        groups=handler.get_ppl_groups(),
        step_func=next_step
    )


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
    __search_step_1(
        what_sg="Text",
        what_pl="Texts",
        selection_keys=list(handler.texts),
        search_func=handler.search_manuscripts_containing_texts,
        state_func=state.store_ms_by_txt_search_state
    )


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
    __show_as_list([handler.manuscripts[x] for x in results])
    def next_step() -> None: state.steps.search_mss_by_txt = Step.MS_by_Txt.Search_Txt
    __save_group(
        ids=results,
        searchterms=txt,
        grouptype=GroupType.ManuscriptGroup,
        step_func=next_step
    )
    __add_to_group(
        ids=results,
        searchterms=txt,
        groups=handler.get_ms_groups(),
        step_func=next_step
    )
    metadatahandler.process_ms_results(handler, results)


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
    __search_step_1(
        what_sg="Text",
        what_pl="Texts",
        selection_keys=list(handler.manuscripts.keys()),
        search_func=handler.search_texts_contained_by_manuscripts,
        state_func=state.store_txt_by_ms_search_state,
        format_func=lambda x: f"{' / '.join(handler.manuscripts[x])} ({x})",
    )


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
    __show_as_list(results)
    def next_step() -> None: state.steps.search_txt_by_mss = Step.Txt_by_Ms.Search_Ms
    __save_group(
        ids=results,
        searchterms=mss,
        grouptype=GroupType.TextGroup,
        step_func=next_step
    )
    __add_to_group(
        ids=results,
        searchterms=mss,
        groups=handler.get_txt_groups(),
        step_func=next_step
    )


# helper functions

def __search_step_1(
    what_sg: str,
    what_pl: str,
    selection_keys: list[str],
    search_func: Callable[[list[str], SearchOptions], list[str]],
    state_func: Callable[[list[str], list[str], SearchOptions], None],
    format_func: Callable[[str], str] = str,
) -> None:
    """Generic function for the first step of a search. May be called by more specific search step functions.

    The parameters are strings for search specific UI displaying, or smaller functions of specific logic, 
    that can be executed here.  
    The aim of this function is to not have to repeat the Streamlit boilerplate four times, or all four search options.

    Args:
        what_sg (str): The thing that is being searched. Singular. (Manuscript/Person/Text)
        what_pl (str): The thing that is being searched. Plural. (Manuscripts/People/Texts)
        selection_keys (list[str]): the search options that are being displayed in the multiselect.
        search_func (Callable[[list[str], SearchOptions], list[str]]): The datahandler's search function 
            `((search_ids, search_mode) => result_ids)` that will return the appropriate search results.
        state_func (Callable[[list[str], list[str], SearchOptions], None]): A function that sets the state 
            to what it should be, once the search is done.
        format_func (Callable[[str], str], optional): A function that formats the selection keys for displaying.
    """
    with st.form(f"search_ms_by_{what_sg}"):
        st.subheader(f"Select {what_sg}(s)")
        mode = __ask_for_search_mode()
        selection = st.multiselect(f'Select {what_sg}', selection_keys, format_func=format_func)
        if st.form_submit_button(f"Search {what_pl}"):
            log.debug(f'Search Mode: {mode}')
            log.debug(f'selection: {selection}')
            with st.spinner('Searching...'):
                res = search_func(selection, mode)
            state_func(res, selection, mode)
            st.experimental_rerun()


def __save_group(
    ids: list[str],
    searchterms: list[str],
    grouptype: GroupType,
    step_func: Callable[[], None]
) -> None:
    """Prompt to save search results as a new Group.

    Args:
        ids (list[str]): the search result IDs
        searchterms (list[str]): the terms that had been searched
        grouptype (GroupType): The group type the new group would be of
        step_func (Callable[[], None]): a function to set the app state to what it should be afterwards
    """
    with st.expander("Save results as group", False):
        with st.form("save_group"):
            name = st.text_input('Group Name', f'Search results for <{searchterms}>')
            if st.form_submit_button("Save"):
                grp = Group(grouptype, name, set(ids))
                handler.put_group(grp)
                step_func()
                st.experimental_rerun()


def __add_to_group(
    ids: list[str],
    searchterms: list[str],
    groups: list[Group],
    step_func: Callable[[], None]
) -> None:
    """Prompt to save search results combined with an existing group.

    Args:
        ids (list[str]): the search result IDs
        searchterms (list[str]): the terms that had been searched
        groups (list[Group]): the existing groups of the applicable group type
        step_func (Callable[[], None]): a function to set the app state to what it should be afterwards
    """
    if not groups:
        return
    with st.expander("Add results to existing group", False):
        with st.form("add_to_group"):
            group_lookup = {g.name: g for g in groups}
            group_names = list(group_lookup.keys())
            previous_name: str = st.radio("Select a group", group_names)
            mode = __ask_for_search_mode()
            previous_group = group_lookup[previous_name]
            previous_query = previous_name.removeprefix("Search results for <").removesuffix(">")
            name = st.text_input('Group Name', f'Search results for <{searchterms} {mode.value} ({previous_query})>')
            if st.form_submit_button("Save"):
                if not previous_group:
                    return
                if mode == SearchOptions.CONTAINS_ALL:
                    new_items = previous_group.items.intersection(set(ids))
                else:
                    new_items = previous_group.items.union(set(ids))
                new_group = Group(previous_group.group_type,  name, new_items)
                handler.put_group(new_group)
                step_func()
                st.experimental_rerun()


def __ask_for_search_mode() -> SearchOptions:
    modes = {'AND (must contain all selected)': SearchOptions.CONTAINS_ALL,
             'OR  (must contain at least one of the selected)': SearchOptions.CONTAINS_ONE}
    mode_selection = st.radio('Search mode', list(modes.keys()), 1)
    return modes[mode_selection]


def __show_as_list(res: list[Any]) -> None:
    with st.expander('view results as list', False):
        st.write(res)


search_page()
