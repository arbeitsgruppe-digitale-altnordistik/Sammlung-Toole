from logging import Logger
import streamlit as st
from util import utils
from util.datahandler import DataHandler
from util.stateHandler import StateHandler


@st.experimental_singleton
def get_log() -> Logger:
    return utils.get_logger(__name__)


def browse_groups(state: StateHandler) -> None:
    handler: DataHandler = state.data_handler
    groups = handler.groups
    st.title("Groups")

    st.header("Manuscript Groups")
    mss = [(b.name, f"{len(b.items)} Manuscripts") for a, b in groups.manuscript_groups.items()]
    st.table(mss)

    st.header("Text Groups")
    st.table(groups.text_groups)

    st.header("People Groups")
    st.table(groups.person_groups)
