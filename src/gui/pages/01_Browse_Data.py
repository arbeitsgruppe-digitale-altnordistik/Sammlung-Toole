import streamlit as st

from src.gui.gui_utils import get_handler


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
# st.dataframe(txt.head())
with st.expander("Show all texts"):
    st.write(txt)
# if st.button("Show text counts"):
#     counts = txt.apply(
#         lambda x: pd.Series({"count": x[x == True].count()})).transpose().sort_values(
#         by=['count'],
#         ascending=False).reset_index().rename(
#         columns={"index": "text"})
#     st.write(counts)
# TODO: Implement again!

# Persons
pers = handler.get_all_ppl_data()
st.header("Persons")
st.write(f'{len(pers)} people loaded.')
with st.expander("Show all people"):
    st.write(pers)
