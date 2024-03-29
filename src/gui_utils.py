from logging import Logger

import streamlit as st

from lib import utils
from lib.datahandler import DataHandler
from lib.stateHandler import StateHandler


@st.experimental_singleton
def get_log() -> Logger:
    return utils.get_logger("GUI")


@st.experimental_singleton
def get_state() -> StateHandler:
    return StateHandler()


@st.experimental_singleton
def get_handler() -> DataHandler:
    return _get_handler()


@st.experimental_singleton
def _get_handler() -> DataHandler:
    with st.spinner('Grabbing data handler...'):
        return DataHandler.make()
