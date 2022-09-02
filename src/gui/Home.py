from typing import Any, Callable, Optional
import numpy as np
import streamlit as st
import pandas as pd
from datetime import datetime
from src.gui.gui_utils import get_log, get_state, get_handler
from src.lib import utils
from src.lib.constants import IMAGE_HOME
from src.lib.stateHandler import StateHandler
from src.lib.utils import Settings
from src.lib.datahandler import DataHandler
from src.gui.pgs import groups
from src.gui.pgs import search
from src.lib.guiUtils import Texts


def mainPage() -> None:
    '''Landing page'''
    st.title("Welcome to Sammlung Toole")
    st.write("The Menu on the left has all the options")
    st.image(IMAGE_HOME)


log = get_log()
state = get_state()
dataHandler = get_handler()
settings = Settings.get_settings()

mainPage()


def search_page(a: Any) -> None:
    st.header('Search Page')
    opts: dict[str, Callable[[StateHandler], None]] = {
        'How To': search.how_to,
        # 'Handrit URLs': handrit_urls,
        'Search Manuscripts by related People': search.manuscripts_by_persons,
        'Search People by related Manuscripts': search.persons_by_manuscripts,
        'Search Manuscripts by Text': search.manuscripts_by_texts,
        'Search Texts contained by Manuscripts': search.text_by_manuscripts,
    }
    st.sidebar.write("---")
    choice = st.sidebar.radio('What would you like to search?', options=list(opts.keys()))
    fn = opts[choice]
    fn(state)
