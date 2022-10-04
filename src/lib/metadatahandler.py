from datetime import datetime as dt

import pandas as pd
import streamlit as st
from src.lib import utils
from src.lib.datahandler import DataHandler
from st_aggrid import AgGrid as ag
from st_aggrid import GridUpdateMode


def citavi_export(metadata: pd.DataFrame) -> None:
    csv = metadata.to_csv(index=False)
    tstamp = dt.now().strftime("%Y-%m-%d-%H%M")
    st.download_button(label="Download", data=csv, file_name=f"toole-citave-export{tstamp}.csv")


def plot_date_scatter(metadata: pd.DataFrame) -> None:
    if metadata.empty:
        return
    fig = utils.date_plotting(metadata)
    st.plotly_chart(fig, use_container_width=True)


def plot_dims(metadata: pd.DataFrame) -> None:
    if metadata.empty:
        return
    fig = utils.dimensions_plotting(metadata)
    if fig:
        st.plotly_chart(fig, use_container_width=True)


def plot_dims_facet(metadata: pd.DataFrame) -> None:
    if metadata.empty:
        return
    fig = utils.dimensions_plotting_facet(metadata)
    if fig:
        st.plotly_chart(fig, use_container_width=True)


def process_ms_results(handler: DataHandler, mss: list[str]) -> None:
    try:
        meta = handler.search_manuscript_data(mss).reset_index(drop=True)
        with st.expander("Show results as table"):
            ag(meta, reload_data=False, update_mode=GridUpdateMode.NO_UPDATE)
    except Exception as e:
        meta = None
        print('Uh-oh:', e)  # TODO: Proper handling of empty results from AND queries.
    if isinstance(meta, pd.DataFrame):
        with st.expander("Export results to Citavi"):
            citavi_export(meta)
        with st.expander("Plot dating of manuscripts"):
            show_data_chart(meta)


def show_data_table(meta: pd.DataFrame) -> None:
    ag(meta, reload_data=False, update_mode=GridUpdateMode.NO_UPDATE)


def show_data_chart(meta: pd.DataFrame) -> None:
    plot_date_scatter(meta)
    plot_dims(meta)
    plot_dims_facet(meta)
