import streamlit as st
import pandas as pd
from datetime import datetime as dt
from util.datahandler import DataHandler
from st_aggrid import AgGrid as ag
from util import utils
from util.stateHandler import StateHandler


def citavi_export(metadata: pd.DataFrame) -> None:
    csv = metadata.to_csv(index=False)
    tstamp = dt.now().strftime("%Y-%m-%d-%H%M")
    st.download_button(label="Download", data=csv, file_name=f"toole-citave-export{tstamp}.csv")


def plotting(metadata: pd.DataFrame) -> None:
    fig = utils.date_plotting(metadata)
    st.plotly_chart(fig, use_container_width=True)


def process_ms_results(state: StateHandler, mss: list[str]) -> None:
    handler = state.data_handler
    try:
        meta = handler.search_manuscript_data(mss).reset_index(drop=True)
        with st.expander("Show results as table"):
            ag(meta)
    except:
        meta = None
        print('Uh-oh')  # TODO: Proper handling of empty results from AND queries.
    if isinstance(meta, pd.DataFrame):
        with st.expander("Export results to Citavi"):
            citavi_export(meta)
        with st.expander("Plot dating of manuscripts"):
            plotting(meta)
