from logging import Logger
import streamlit as st
from util import utils
from util.datahandler import DataHandler
from util.stateHandler import StateHandler, Step


@st.experimental_singleton  # type: ignore
def get_log() -> Logger:
    return utils.get_logger(__name__)


log: Logger = get_log()


def browse_groups(state: StateHandler) -> None:
    handler: DataHandler = state.data_handler
    groups = handler.groups
    log.debug(f"Browsing Groups: {groups}")
    st.title("Groups")
    if state.steps.browseGroups == Step.Browse_Groups.Browse:
        st.header("Manuscript Groups")
        mss = [(b.name, f"{len(b.items)} Manuscripts", b.date.strftime('%c')) for a, b in groups.manuscript_groups.items()]
        st.table(mss)
        if st.button("Combine existing groups to a new group"):
            state.steps.browseGroups = Step.Browse_Groups.Combine_MSS
            st.experimental_rerun()
        st.header("Text Groups")
        txt = [(b.name, f"{len(b.items)} Texts", b.date.strftime('%c')) for a, b in groups.text_groups.items()]
        st.table(txt)
        st.header("People Groups")
        ppl = [(b.name, f"{len(b.items)} People", b.date.strftime('%c')) for a, b in groups.person_groups.items()]
        st.table(ppl)
    elif state.steps.browseGroups == Step.Browse_Groups.Combine_MSS:
        st.header("Combine Manuscript Groups")
        if st.button("Back"):
            state.steps.browseGroups = Step.Browse_Groups.Browse
            st.experimental_rerun()
        # TODO: implement
