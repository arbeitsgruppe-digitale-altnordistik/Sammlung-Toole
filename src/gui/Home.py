import streamlit as st
from src.lib.constants import IMAGE_HOME
st.set_page_config(layout="wide")

st.title("Welcome to Sammlung Toole")
st.write("The Menu on the left has all the options")
st.image(IMAGE_HOME)
st.markdown("For more information, check the help page, or view the " +
            "[documentation](https://arbeitsgruppe-digitale-altnordistik.github.io/Sammlung-Toole/).")
