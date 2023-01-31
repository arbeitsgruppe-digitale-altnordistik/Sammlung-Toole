import streamlit as st

from gui_utils import get_handler


handler = get_handler()
st.title("Currently Loaded Dataset")

# Manuscripts
mss = handler.manuscripts
st.header("Manuscripts")
st.write(f"We have {len(mss)} mss")
st.write("Each manuscript can have entries in multiple languages (English, Icelandic, Danish)")
with st.expander("Show all manuscripts"):
    st.write(list(mss.keys()))

# Texts
txt = handler.texts
st.header("Texts")
st.write(f'Found {len(txt)} texts.')
with st.expander("Show all texts"):
    st.write(txt)

# Persons
pers = handler.person_names
st.header("Persons")
st.write(f'{len(pers)} people loaded.')
with st.expander("Show all people"):
    st.write(pers)
