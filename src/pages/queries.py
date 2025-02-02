import streamlit as st
from database.querries import QueryExecutor

query = st.text_input("Query")
if st.button("Execute"):
    st.success(QueryExecutor.execute_query(query))