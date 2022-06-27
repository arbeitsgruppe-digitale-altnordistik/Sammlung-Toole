import streamlit as st
import pandas as pd
from datetime import datetime as dt
from util import utils


def citavi_export(metadata: pd.DataFrame):
    csv = metadata.to_csv(index=False)
    tstamp = dt.now().strftime("%Y-%m-%d-%H%M")
    st.download_button(label="Download", data=csv, file_name=f"toole-citave-export{tstamp}.csv")


def plotting(metadata: pd.DataFrame):
    fig = utils.date_plotting(metadata)
    st.plotly_chart(fig, use_container_width=True)
