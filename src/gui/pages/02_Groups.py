from uuid import UUID
from src.gui.gui_utils import get_log, get_handler, get_state
from src.lib.groups import Group
from src.lib.stateHandler import Step
import streamlit as st
from src.lib import metadatahandler
from src.lib.utils import SearchOptions

COMBINE_GROUPS = "Combine existing groups to a new group"

log = get_log()
state = get_state()
handler = get_handler()


def browse_groups() -> None:
    """Page: Browse Groups

    This page of the streamlit app allows browsing the different groups stored in the data handler.

    Args:
        state (StateHandler): The StateHandler object orchestrating the current session state.
    """
    st.title("Groups")
    step = state.steps.browseGroups
    if step == Step.Browse_Groups.Browse:
        __step_browse_groups()
    elif state.steps.browseGroups == Step.Browse_Groups.Combine_MSS:
        __combine_groups(
            header="Combine Manuscript Groups",
            groups=handler.groups.manuscript_groups
        )
    elif state.steps.browseGroups == Step.Browse_Groups.Combine_TXT:
        __combine_groups(
            header="Combine Text Groups",
            groups=handler.groups.text_groups
        )
    elif state.steps.browseGroups == Step.Browse_Groups.Combine_PPL:
        __combine_groups(
            header="Combine Person Groups",
            groups=handler.groups.person_groups
        )
    elif step == Step.Browse_Groups.Meta_MSS:
        __meta_mss_groups()


def __combine_groups(header: str, groups: dict[UUID, Group]) -> None:
    st.header(header)
    if st.button("Back to Overview"):
        state.steps.browseGroups = Step.Browse_Groups.Browse
        st.experimental_rerun()
    st.write("---")
    sel = __group_selector(groups)
    if len(sel) > 1:
        combMode = __union_selector()
        selComb = __group_combinator(sel, combMode)
        if not selComb:
            st.write("0 results for selected criteria. Tip: Try OR instead of AND")
        else:
            res = selComb
            st.write(f"Found {len(res)} items to combine to a new group")
            with st.expander("Create new group"):
                __save_merged_group(combo=res, mode=combMode, selected_groups=sel)
    else:
        st.write("Select two or more groups you want to combine")


def __step_browse_groups() -> None:
    groups = handler.groups
    st.header("Manuscript Groups")
    mss = [(b.name, f"{len(b.items)} Manuscripts", b.date.strftime('%c')) for _, b in groups.manuscript_groups.items()]
    st.table(mss)
    if len(mss) >= 2 and st.button(COMBINE_GROUPS, key="btn_combine_mss"):
        state.steps.browseGroups = Step.Browse_Groups.Combine_MSS
        st.experimental_rerun()
    if mss and st.button("Get metadata for group(s)"):
        state.steps.browseGroups = Step.Browse_Groups.Meta_MSS
        st.experimental_rerun()
        # Text Groups
    st.header("Text Groups")
    txt = [(b.name, f"{len(b.items)} Texts", b.date.strftime('%c')) for _, b in groups.text_groups.items()]
    st.table(txt)
    if len(txt) >= 2 and st.button(COMBINE_GROUPS, key="btn_combine_txt"):
        state.steps.browseGroups = Step.Browse_Groups.Combine_TXT
        st.experimental_rerun()
    # Person Groups
    st.header("People Groups")
    ppl = [(b.name, f"{len(b.items)} People", b.date.strftime('%c')) for _, b in groups.person_groups.items()]
    st.table(ppl)
    if len(ppl) >= 2 and st.button(COMBINE_GROUPS, key="btn_combine_ppl"):
        state.steps.browseGroups = Step.Browse_Groups.Combine_PPL
        st.experimental_rerun()


def __meta_mss_groups() -> None:
    """Page for getting metadata and playground for MSS group(s)"""
    st.header("Get metadata for group(s)")
    sel = __group_selector()
    if sel:
        res = None
        if len(sel) > 1:
            combMode = __union_selector()
            selComb = __group_combinator(sel, combMode)
            if not selComb:
                st.write("No manuscripts for selected criteria. Tip: Try OR instead of AND")
            else:
                res = list(selComb)
        elif len(sel) == 1:
            res = list(sel[0].items)
        if res:
            st.write(f"Found {len(res)} manuscripts")
            if st.button("Load metadata and stuff"):
                metadatahandler.process_ms_results(handler, res)
    else:
        st.write("Select one or more groups you want to work with")


def __group_selector(groups: dict[UUID, Group] = handler.groups.manuscript_groups) -> list[Group]:
    st.write("Select the groups you want to combine.")
    selections: set[UUID] = set()
    for i, g in enumerate(groups.values()):
        if st.checkbox(f"{i}: Group name: '{g.name}'", key=str(g.group_id)):
            selections.add(g.group_id)
    selected_groups = [groups[s] for s in selections]
    return selected_groups


def __group_combinator(groups: list[Group], mode: SearchOptions) -> set[str]:
    sets = [g.items for g in groups]
    st.write("---")
    if mode == SearchOptions.CONTAINS_ONE:
        res = set.union(*sets)
    else:
        res = set.intersection(*sets)
    return res


def __union_selector() -> SearchOptions:
    modes = {
        'OR  (union - pick items that appear in at least one selected group)': SearchOptions.CONTAINS_ONE,
        'AND (intersection - pick items that appear in all selected groups)': SearchOptions.CONTAINS_ALL,
    }
    mode_selection = st.radio("Combination mode", list(modes.keys()))
    mode = modes[mode_selection]
    return mode


def __save_merged_group(combo: set[str], mode: SearchOptions, selected_groups: list[Group]) -> None:
    group_type = selected_groups[0].group_type
    previous_queries = ['(' + prev.name.removeprefix("Search results for <").removesuffix(">") + ')' for prev in selected_groups]
    new_query = f" {mode.value} ".join(previous_queries)
    new_name = f'Search results for <{new_query}>'
    name = st.text_input(label="Select a group name", value=new_name)
    if st.button("Save Combined Group"):
        new_group = Group(group_type=group_type, name=name, items=combo)
        handler.groups.set(new_group)
        state.steps.browseGroups = Step.Browse_Groups.Browse
        st.experimental_rerun()


browse_groups()
