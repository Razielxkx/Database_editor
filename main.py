from src.database.models import session, TableFactory
import streamlit as st
from src.database.table_model import TableModel

st.set_page_config(page_title="Db editor", layout="wide")
st.title("Database editor")

try:
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

    st.header("Create a New Table")
    table_name = st.text_input("Table name")
    if table_name:
        col_name = st.text_input("Column name")
        col_type = st.text_input("Column type")
        nullable = st.checkbox("Nullable")

        if st.button("Add column"):
            if col_name and col_type:
                # Add column to the session state dictionary
                model = TableModel(col_name, col_type, nullable)
                st.session_state.columns.append(model)
                st.success(f"Added column: {col_name} ({col_type})")
            else:
                st.error("Please provide both Column name and Column type.")

        if st.button("Create table"):
            TableFactory.create_table_class(table_name, st.session_state.columns)
            st.success(f"Table '{table_name}' created with columns: {st.session_state.columns}")
            st.session_state.columns = {}

except Exception as ex:
    print(f"Something went wrong with error: {ex}")
finally:
    session.close()