from logging import Logger
from typing import Set
from uuid import UUID
import streamlit as st
from util import utils
from util.datahandler import DataHandler
from util.groups import Group, GroupType
from util.stateHandler import StateHandler, Step
from util.utils import SearchOptions


@st.experimental_singleton  # type: ignore
def get_log() -> Logger:
    return utils.get_logger(__name__)


log: Logger = get_log()


def browse_groups(state: StateHandler) -> None:
    """Page: Browse Groups

    This page of the streamlit app allows browsing the different groups stored in the data handler.

    Args:
        state (StateHandler): The StateHandler object orchestrating the current session state.
    """
    handler: DataHandler = state.data_handler
    groups = handler.groups
    log.debug(f"Browsing Groups: {groups}")
    st.title("Groups")
    if state.steps.browseGroups == Step.Browse_Groups.Browse:
        # Manuscript Groups
        st.header("Manuscript Groups")
        mss = [(b.name, f"{len(b.items)} Manuscripts", b.date.strftime('%c')) for a, b in groups.manuscript_groups.items()]
        st.table(mss)
        if len(mss) >= 2 and st.button("Combine existing groups to a new group", key="btn_combine_mss"):
            state.steps.browseGroups = Step.Browse_Groups.Combine_MSS
            st.experimental_rerun()
        # Text Groups
        st.header("Text Groups")
        txt = [(b.name, f"{len(b.items)} Texts", b.date.strftime('%c')) for a, b in groups.text_groups.items()]
        st.table(txt)
        if len(txt) >= 2 and st.button("Combine existing groups to a new group", key="btn_combine_txt"):
            state.steps.browseGroups = Step.Browse_Groups.Combine_TXT
            st.experimental_rerun()
        # Person Groups
        st.header("People Groups")
        ppl = [(b.name, f"{len(b.items)} People", b.date.strftime('%c')) for a, b in groups.person_groups.items()]
        st.table(ppl)
        if len(ppl) >= 2 and st.button("Combine existing groups to a new group", key="btn_combine_ppl"):
            state.steps.browseGroups = Step.Browse_Groups.Combine_PPL
            st.experimental_rerun()
    elif state.steps.browseGroups == Step.Browse_Groups.Combine_MSS:
        __combine_mss_groups(state)
    elif state.steps.browseGroups == Step.Browse_Groups.Combine_TXT:
        __combine_txt_groups(state)
    elif state.steps.browseGroups == Step.Browse_Groups.Combine_PPL:
        __combine_ppl_groups(state)


def __combine_mss_groups(state: StateHandler) -> None:
    groups = state.data_handler.groups.manuscript_groups
    st.header("Combine Manuscript Groups")
    if st.button("Back to Overview"):
        state.steps.browseGroups = Step.Browse_Groups.Browse
        st.experimental_rerun()
    st.write("---")
    modes = {
        'OR  (union - pick items that appear in at least one selected group)': SearchOptions.CONTAINS_ONE,
        'AND (intersection - pick items that appear in all selected groups)': SearchOptions.CONTAINS_ALL,
    }
    mode_selection = st.radio('Combination mode', modes.keys())
    mode = modes[mode_selection]
    st.write("---")
    st.write("Select the groups you want to combine.")
    selections: Set[UUID] = set()
    for i, g in enumerate(groups.values()):
        if st.checkbox(f"{i}: Group name: '{g.name}'", key=str(g.group_id)):
            selections.add(g.group_id)
    st.write("---")
    if len(selections) > 1:
        selected_groups = [groups[s] for s in selections]
        sets = [g.items for g in selected_groups]
        if mode == SearchOptions.CONTAINS_ONE:
            res = set.union(*sets)
        else:
            res = set.intersection(*sets)
        if not res:
            st.write("No Manuscripts fitting the criteria. (Maybe consider using OR instead of AND for combination logic.)")
        else:
            st.write(f"The combination contains {len(res)} Manuscripts.")
            previous_queries = ['(' + prev.name.removeprefix("Search results for <").removesuffix(">") + ')' for prev in selected_groups]
            new_query = f" {mode.value} ".join(previous_queries)
            new_name = f'Search results for <{new_query}>'
            name = st.text_input(label="Select a group name", value=new_name)
            if st.button("Save Combined Group"):
                new_group = Group(GroupType.ManuscriptGroup, name=name, items=res)
                state.data_handler.groups.set(new_group)
                state.steps.browseGroups = Step.Browse_Groups.Browse
                st.experimental_rerun()


def __combine_txt_groups(state: StateHandler) -> None:
    groups = state.data_handler.groups.text_groups
    st.header("Combine Text Groups")
    if st.button("Back to Overview"):
        state.steps.browseGroups = Step.Browse_Groups.Browse
        st.experimental_rerun()
    st.write("---")
    modes = {
        'OR  (union - pick items that appear in at least one selected group)': SearchOptions.CONTAINS_ONE,
        'AND (intersection - pick items that appear in all selected groups)': SearchOptions.CONTAINS_ALL,
    }
    mode_selection = st.radio('Combination mode', modes.keys())
    mode = modes[mode_selection]
    st.write("---")
    st.write("Select the groups you want to combine.")
    selections: Set[UUID] = set()
    for i, g in enumerate(groups.values()):
        if st.checkbox(f"{i}: Group name: '{g.name}'", key=str(g.group_id)):
            selections.add(g.group_id)
    st.write("---")
    if len(selections) > 1:
        selected_groups = [groups[s] for s in selections]
        sets = [g.items for g in selected_groups]
        if mode == SearchOptions.CONTAINS_ONE:
            res = set.union(*sets)
        else:
            res = set.intersection(*sets)
        if not res:
            st.write("No Texts fitting the criteria. (Maybe consider using OR instead of AND for combination logic.)")
        else:
            st.write(f"The combination contains {len(res)} Texts.")
            previous_queries = ['(' + prev.name.removeprefix("Search results for <").removesuffix(">") + ')' for prev in selected_groups]
            new_query = f" {mode.value} ".join(previous_queries)
            new_name = f'Search results for <{new_query}>'
            name = st.text_input(label="Select a group name", value=new_name)
            if st.button("Save Combined Group"):
                new_group = Group(GroupType.TextGroup, name=name, items=res)
                state.data_handler.groups.set(new_group)
                state.steps.browseGroups = Step.Browse_Groups.Browse
                st.experimental_rerun()


def __combine_ppl_groups(state: StateHandler) -> None:
    groups = state.data_handler.groups.person_groups
    st.header("Combine Person Groups")
    if st.button("Back to Overview"):
        state.steps.browseGroups = Step.Browse_Groups.Browse
        st.experimental_rerun()
    st.write("---")
    modes = {
        'OR  (union - pick items that appear in at least one selected group)': SearchOptions.CONTAINS_ONE,
        'AND (intersection - pick items that appear in all selected groups)': SearchOptions.CONTAINS_ALL,
    }
    mode_selection = st.radio('Combination mode', modes.keys())
    mode = modes[mode_selection]
    st.write("---")
    st.write("Select the groups you want to combine.")
    selections: Set[UUID] = set()
    for i, g in enumerate(groups.values()):
        if st.checkbox(f"{i}: Group name: '{g.name}'", key=str(g.group_id)):
            selections.add(g.group_id)
    st.write("---")
    if len(selections) > 1:
        selected_groups = [groups[s] for s in selections]
        sets = [g.items for g in selected_groups]
        if mode == SearchOptions.CONTAINS_ONE:
            res = set.union(*sets)
        else:
            res = set.intersection(*sets)
        if not res:
            st.write("No person fitting the criteria. (Maybe consider using OR instead of AND for combination logic.)")
        else:
            st.write(f"The combination contains {len(res)} people.")
            previous_queries = ['(' + prev.name.removeprefix("Search results for <").removesuffix(">") + ')' for prev in selected_groups]
            new_query = f" {mode.value} ".join(previous_queries)
            new_name = f'Search results for <{new_query}>'
            name = st.text_input(label="Select a group name", value=new_name)
            if st.button("Save Combined Group"):
                new_group = Group(GroupType.PersonGroup, name=name, items=res)
                state.data_handler.groups.set(new_group)
                state.steps.browseGroups = Step.Browse_Groups.Browse
                st.experimental_rerun()
