import os
import re
from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, DECIMAL, DateTime,
    MetaData, Table, inspect
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from database.table_model import TableModel

# Valid column types for table creation
VALID_COL_TYPES = ("int", "integer", "bool", "boolean", "decimal", "money", "datetime")
STRING_COL_TYPES = ("str", "varchar", "nvarchar", "string")

# Load environment variables
load_dotenv()

db_url_for_mysql = (
    f"mysql+mysqlconnector://{os.getenv('MYSQL_USERNAME')}:{os.getenv('MYSQL_PASSWORD')}"
    "@localhost:3306/php_my_admin"
)
engine = create_engine(db_url_for_mysql)
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()


class TableFactory:
    """
    Factory class for dynamically creating, dropping, and inspecting database tables.
    """

    @staticmethod
    def map_column_types(col_type: str):
        """
        Maps a string-based column type to the corresponding SQLAlchemy column type.

        Args:
            col_type (str): The column type as a string.

        Returns:
            SQLAlchemy column type class (e.g., Integer, String, Boolean, etc.)
        """
        col_type = col_type.lower()
        if col_type in ("int", "integer"):
            return Integer
        if col_type in ("bool", "boolean"):
            return Boolean
        if col_type in ("decimal", "money"):
            return DECIMAL
        if col_type == "datetime":
            return DateTime
        if col_type.startswith(STRING_COL_TYPES):
            match = re.search(r'\((\d+)\)', col_type)
            return String(int(match.group(1))) if match else String
        return None

    @staticmethod
    def create_table_class(table_name: str, columns: list[TableModel]):
        """
        Dynamically creates a SQLAlchemy table class based on provided columns.

        Args:
            table_name (str): Name of the table to create.
            columns (list[TableModel]): List of TableModel objects representing table columns.

        Returns:
            A dynamically created SQLAlchemy table class.
        """
        table_attrs = {"__tablename__": table_name}
        for model in columns:
            table_attrs[model.col_name] = Column(
                TableFactory.map_column_types(model.col_type),
                primary_key=(model.col_name == "id"),
                nullable=model.nullable
            )

        table = type(table_name, (Base,), table_attrs)
        Base.metadata.create_all(engine)
        return table

    @staticmethod
    def drop_table(table_name: str):
        """
        Drops a table from the database if it exists.

        Args:
            table_name (str): The name of the table to drop.

        Returns:
            str: A message indicating success or failure.
        """
        metadata = MetaData()
        try:
            table = Table(table_name, metadata, autoload_with=engine)
            table.drop(engine)
            return f"Table '{table_name}' has been dropped."
        except Exception as e:
            return f"Error dropping table '{table_name}': {e}"

    @staticmethod
    def get_all_tables():
        """
        Retrieves a list of all tables in the database.

        Returns:
            dict: Dictionary of table names mapped to SQLAlchemy Table objects.
        """
        metadata = MetaData()
        metadata.reflect(bind=engine)
        return metadata.tables

    @staticmethod
    def get_table_columns(table_name: str):
        """
        Retrieves column names and types for a given table.

        Args:
            table_name (str): The name of the table to inspect.

        Returns:
            dict: Dictionary of column names mapped to their data types.
        """
        inspector = inspect(engine)
        return {column["name"]: column["type"] for column in inspector.get_columns(table_name)}

    @staticmethod
    def valid_col_type(col_type: str):
        """
        Validates if the given column type is supported.

        Args:
            col_type (str): The column type as a string.

        Returns:
            bool: True if valid, False otherwise.
        """
        for string_type in STRING_COL_TYPES:
            if re.fullmatch(rf"{string_type}\(\d+\)", col_type):
                return True
        return col_type in VALID_COL_TYPES


if __name__ == "__main__":
    table_columns = [
        TableModel("id", "int", False),
        TableModel("firstname", "string(50)", False),
        TableModel("surname", "string(50)", False),
        TableModel("married", "bool", False),
        TableModel("created_on", "datetime", False),
        TableModel("salary", "money", False)
    ]

    TableFactory.create_table_class("complex_table_with_objects", table_columns)
