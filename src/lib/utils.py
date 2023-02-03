from __future__ import annotations

import logging
import os
import sys
from enum import Enum
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
import statsmodels.api as sm
from plotly.graph_objs import Figure

__logs: list[logging.Logger] = []


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


__last: Optional[Settings] = None


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

    log_format = logging.Formatter('%(asctime)s [ %(name)s ] - %(levelname)s:   %(message)s')

    if not os.path.exists('logs'):
        os.mkdir('logs')

    f_handler = logging.FileHandler('logs/warnings.log', mode='a', encoding='utf-8')
    f_handler.setLevel(logging.WARNING)
    f_handler.setFormatter(log_format)
    log.addHandler(f_handler)

    f_handler2 = logging.FileHandler('logs/log.log', mode='a', encoding='utf-8')
    f_handler2.setLevel(logging.DEBUG)
    f_handler2.setFormatter(log_format)
    log.addHandler(f_handler2)

    c_h = logging.StreamHandler(sys.stdout)
    c_h.setLevel(logging.INFO)
    c_h.setFormatter(log_format)
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

# LATER: maybe have a separate module for plotting stuff/reports

def date_plotting(df: pd.DataFrame) -> Figure:
    ''' Plots the data of a given set of MSs. Used with MS metadata results. Returns scatterplot.
    Args:
        inDF(dataFrame, required): pandas DataFrame
    Returns:
        scatterplot data for plotly to be drawn with corresponding function
    '''
    df = df[df['terminus_ante_quem'] != 0]
    df = df[df['terminus_post_quem'] != 0]
    fig = px.scatter(df, x='terminus_post_quem', y='terminus_ante_quem', color='shelfmark')
    first = df['terminus_post_quem'].min()-20
    last = df['terminus_ante_quem'].max()+20
    fig.add_shape(type='line', x0=first, y0=first, x1=last, y1=last, line=dict(color='rgba(50,50,50,0.8)'))
    fig.update_layout(
        title="Manuscript Dating Plot",
        xaxis_title="Terminus Post Quem",
        yaxis_title="Terminus Ante Quem",
        legend_title="Manuscripts"
    )
    return fig


def dimensions_plotting(df: pd.DataFrame) -> Optional[Figure]:
    df["width"] = pd.to_numeric(df["width"], errors='coerce')
    df["height"] = pd.to_numeric(df["height"], errors='coerce')
    df = df[df['height'] != 0]
    df = df[df['width'] != 0]
    df = df[df['date_mean'] != 0]
    if df.empty:
        return None
    fig = px.scatter(
        df,
        x='width',
        y='height',
        color=df['support'],
        hover_name=df['shelfmark']
    )
    return fig


def dimensions_plotting_facet(df: pd.DataFrame) -> Optional[Figure]:
    df = df[['width', 'height', 'date_mean', 'support', 'shelfmark']]
    df["width"] = pd.to_numeric(df["width"], errors='coerce')
    df["height"] = pd.to_numeric(df["height"], errors='coerce')
    df = df.dropna()
    df = df[df['height'] != 0]
    df = df[df['width'] != 0]
    df = df[df['date_mean'] != 0]
    if df.empty:
        return None
    df['century'] = df['date_mean'].div(100).round()
    df = df.sort_values('width')
    model = sm.OLS(df["height"], sm.add_constant(df["width"])).fit()
    trace = go.Scatter(x=df["width"], y=model.predict(), line_color="gray", name="overall OLS")
    trace.update(legendgroup="trendline", showlegend=False)
    df = df.sort_values('century')
    fig = px.scatter(
        df,
        x='width',
        y='height',
        color=df['support'],
        hover_name=df['shelfmark'],
        facet_col=df['century'],
        facet_col_wrap=3
    )
    fig.add_trace(trace, row="all", col="all", exclude_empty_subplots=True)
    fig.update_traces(selector=-1, showlegend=True)
    return fig
