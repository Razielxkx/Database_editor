import streamlit as st
from database.querries import QueryExecutor
from database.models import session, TableFactory

tables = TableFactory.get_all_tables()
if tables:
    st.sidebar.header("Tables")
    selected_table = st.sidebar.selectbox("Select a table:", tables.keys())

    if selected_table:
        st.sidebar.subheader(f"Columns in '{selected_table}'")
        columns = TableFactory.get_table_columns(selected_table)
        if columns:
            for key, val in columns.items():
                st.sidebar.write(f"- {key} -> {val}")
        else:
            st.sidebar.write("No columns found for this table.")

if "columns" not in st.session_state:
    st.session_state.columns = list()

query = st.text_input("Query")
if st.button("Execute"):
    if "select" in query.lower():
        st.dataframe(QueryExecutor.execute_query(query))
    else:
        st.success(QueryExecutor.execute_query(query))