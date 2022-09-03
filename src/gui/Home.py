import streamlit as st
from src.gui.gui_utils import get_log, get_state, get_handler
from src.lib.constants import IMAGE_HOME
from src.lib.utils import Settings


def main_page() -> None:
    '''Landing page'''
    st.title("Welcome to Sammlung Toole")
    st.write("The Menu on the left has all the options")
    st.image(IMAGE_HOME)


log = get_log()
state = get_state()
dataHandler = get_handler()
settings = Settings.get_settings()

main_page()
