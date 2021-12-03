from __future__ import annotations
import logging
from typing import Any, Dict, List, Optional
from plotly.missing_ipywidgets import FigureWidget
import requests
from bs4 import BeautifulSoup
import sys
import plotly.express as px
import pandas as pd
import os
from enum import Enum
from time import time
import json
import subprocess
from datetime import timedelta


__logs: List[logging.Logger] = []


class SearchOptions(Enum):
    CONTAINS_ALL = "AND"
    """AND search: the item must contain all of the requested elements, in order to fit"""
    CONTAINS_ONE = "OR"
    """OR search: the item must contain at least one of the requested elements, in order to fit"""


class Settings:
    """Data structure to hold settings for the application."""

    def __init__(self) -> None:
        self.__verbose = True
        self.__debug = True
        # self.__max_res = sys.maxsize
        self.cache = True
        self.use_cache = True
        global __last
        __last = self

    @staticmethod
    def get_settings() -> Settings:
        return __last if __last else Settings()

    @property
    def max_res(self) -> int:
        return self.__max_res

    @max_res.setter
    def max_res(self, val: int) -> None:
        self.__max_res = val

    @property
    def verbose(self) -> bool:
        return self.__verbose

    @verbose.setter
    def verbose(self, val: bool) -> None:
        if val:
            set_log_level(verbose=True)
        else:
            self.debug = False
            set_log_level(verbose=False)
        self.__verbose = val

    @property
    def debug(self) -> bool:
        return self.__debug

    @debug.setter
    def debug(self, val: bool) -> None:
        if val:
            self.verbose = True
            set_log_level(debug=True)
        else:
            set_log_level(debug=False)
        self.__debug = val


class GitUtil:
    """ Utility class for dealing with the git submodule """
    _path = ".handlerstate.json"

    @staticmethod
    def __read_data() -> Dict[str, Any]:
        if os.path.exists(GitUtil._path):
            with open(GitUtil._path, mode='r+', encoding='utf-8') as f:
                data: Dict[str, Any] = json.load(f)
            return data
        return {}

    @staticmethod
    def __write_data(data: Dict[str, Any]) -> None:
        # if os.path.exists(GitUtil._path):
        with open(GitUtil._path, mode='w+', encoding='utf-8') as f:
            json.dump(data, f, indent=4)

    @staticmethod
    def update_handler_state() -> None:
        """Updates the hansler state information.

        In `.handlerstate.json`, this stores information of the submodule as the current handler state.  
        Assumes that the handler is up to date.

        This should be called after reloading the handler (ideally without cache), 
        as its data would then reflect the current state of the submodule (which we assume to be mostly up to date).
        """
        submodule_state = GitUtil.__read_data().get("submoduleState") or {}
        proc = subprocess.run("git -C data/handrit show --quiet --format=format:%h".split(), capture_output=True)
        com_hash = str(proc.stdout, 'utf-8')
        obj = {
            "isUpToDate": True,
            "handlerState": {
                "timestamp": int(time()),
                "commitHash": com_hash
            },
            "submoduleState": submodule_state
        }
        GitUtil.__write_data(obj)

    @staticmethod
    def update_submodule_state() -> None:
        """Updates the submodule state.

        In `.handlerstate.json`, this stores the current submodule state.
        Assumes that the handler is out of date.

        Should be called after updating the actual submodule content, if it changed at all.
        Indicates that the handler should be rebuilt as the data has changed. 
        """
        handler_state = GitUtil.__read_data().get("handlerState") or {}
        proc = subprocess.run("git -C data/handrit show --quiet --format=format:%h".split(), capture_output=True)
        com_hash = str(proc.stdout, 'utf-8')
        proc = subprocess.run("git -C data/handrit show --quiet --format=format:%ct".split(), capture_output=True)
        com_time = str(proc.stdout, 'utf-8')
        obj = {
            "isUpToDate": (com_hash == handler_state.get("commitHash")),
            "handlerState": handler_state,
            "submoduleState": {
                "timestamp": int(com_time),
                "commitHash": com_hash
            }
        }
        GitUtil.__write_data(obj)

    @staticmethod
    def get_comparison_link() -> Optional[str]:
        """Provides a comparison link between the handler and the submodule state.

        If possible, returns a link to github,
        where the are compared between the thate currently reflected by the git submodule,
        and the state of the submodule at the time the handler was loaded.

        Returns:
            Optional[str]: Link to github commit comparison, if applicable.
        """
        data = GitUtil.__read_data()
        if data:
            h = data.get("handlerState")
            s = data.get("submoduleState")
            if h and s:
                com_h = h.get("commitHash")
                com_s = s.get("commitHash")
                if com_h and com_s:
                    return f"https://github.com/Handrit/Manuscripts/compare/{com_s}..{com_h}"
        return None

    @staticmethod
    def get_time_difference() -> str:
        """
        Provides a human readable string of how much time passed between the commit of the current state of the submodule,
        and the commit of the submodule at the time the handler was built.
        Empty string, if no time difference can be determined.

        Returns:
            str: human readable string indication the time span
        """
        data = GitUtil.__read_data()
        if data:
            h = data.get("handlerState")
            s = data.get("submoduleState")
            if h and s:
                t_h = h.get("timestamp")
                t_s = s.get("timestamp")
                secs = abs(int(t_h)-int(t_s))
                d = timedelta(seconds=secs)
                return str(d)
        return ""

    @staticmethod
    def is_up_to_date() -> bool:
        """True, if the submodule points to the same commit as it did at the time the handler was built. False otherwise."""
        data = GitUtil.__read_data()
        u = data.get("isUpToDate")
        return (u == True)


__last: Optional[Settings] = None


def get_soup(url: str, parser: str = 'xml') -> BeautifulSoup:
    """Get a BeautifulSoup object from a URL

    Args:
        url (str): The URL
        parser (str, optional): Parser; for HTML, use 'lxml'. Defaults to 'xml'.

    Returns:
        BeautifulSoup: BeautifulSoup object representation of the HTML/XML page.
    """
    __log.debug(f'Requesting ({parser}): {url}')
    htm = requests.get(url).text
    soup = BeautifulSoup(htm, parser)
    return soup


def get_logger(name: str) -> logging.Logger:
    """returns a pre-configured logger"""
    log = logging.getLogger(name)

    global __last
    if __last is None:
        __last = Settings()

    if __last.debug:
        log.setLevel(logging.DEBUG)
    elif __last.verbose:
        log.setLevel(logging.INFO)
    else:
        log.setLevel(logging.WARNING)

    format = logging.Formatter('%(asctime)s [ %(name)s ] - %(levelname)s:   %(message)s')

    if not os.path.exists('logs'):
        os.mkdir('logs')

    f_handler = logging.FileHandler('logs/warnings.log', mode='a', encoding='utf-8')
    f_handler.setLevel(logging.WARNING)
    f_handler.setFormatter(format)
    log.addHandler(f_handler)

    f_handler2 = logging.FileHandler('logs/log.log', mode='a', encoding='utf-8')
    f_handler2.setLevel(logging.DEBUG)
    f_handler2.setFormatter(format)
    log.addHandler(f_handler2)

    c_h = logging.StreamHandler(sys.stdout)
    c_h.setLevel(logging.INFO)
    c_h.setFormatter(format)
    log.addHandler(c_h)

    __logs.append(log)

    return log


def set_log_level(debug: bool = False, verbose: bool = True) -> None:
    """Set Log Levels

    Set the log levels of all created logs of the application (usually three)self.

    If both `debug` and `verbose` are set `False`, the log level will be `logging.WARNING`.

    Default is `logging.INFO`/`verbose`

    Args:
        debug (bool, optional): Set `True` if the new log level should be `logging.DEBUG`. Defaults to False.
        verbose (bool, optional): Set `True` if the new log level should be `logging.INFO`. Defaults to True.
    """
    if debug:
        level = logging.DEBUG
    elif verbose:
        level = logging.INFO
    else:
        level = logging.WARNING

    __log.debug(f"Set log level to: {level}")
    for l in __logs:
        l.setLevel(level)


__log = get_logger(__name__)


# Util functions for interface
# ----------------------------

def date_plotting(inDF: pd.DataFrame) -> FigureWidget:  # TODO Update doc  # LATER: maybe have a separate module for plotting stuff/reports
    ''' Plots the data of a given set of MSs. Used with MS metadata results. Returns scatterplot.
    Args:
        inDF(dataFrame, required): pandas DataFrame
    Returns:
        scatterplot data for plotly to be drawn with corresponding function
    '''

    inDF = inDF[inDF['Terminus ante quem'] != 0]
    inDF = inDF[inDF['Terminus post quem'] != 0]
    fig = px.scatter(inDF, x='Terminus post quem', y='Terminus ante quem', color='shelfmark')
    return fig
