from logging import Logger

import streamlit as st
from src.lib import utils
from src.lib.datahandler import DataHandler
from src.lib.stateHandler import StateHandler


@st.experimental_singleton   # type: ignore
def get_log() -> Logger:
    return utils.get_logger(__name__)


@st.experimental_singleton   # type: ignore
def get_state() -> StateHandler:
    return StateHandler()


def get_handler() -> DataHandler:
    return _get_handler()   # type: ignore


@st.experimental_singleton   # type: ignore
def _get_handler() -> DataHandler:
    with st.spinner('Grabbing data handler...'):
        return DataHandler.get_handler()
