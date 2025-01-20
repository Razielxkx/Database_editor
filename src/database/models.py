import os
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DECIMAL, DateTime, MetaData, Table, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import re
from src.database.table_model import TableModel
from dotenv import load_dotenv

VALID_COL_TYPES = ("int", "integer", "bool", "boolean", "decimal", "money", "str", "varchar", "nvarchar", "string", "datetime")

load_dotenv()
db_url_for_mysql = f"mysql+mysqlconnector://{os.getenv("MYSQL_USERNAME")}:{os.getenv("MYSQL_PASSWORD")}@localhost:3306/php_my_admin"
engine = create_engine(db_url_for_mysql)
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()

class TableFactory:
    @staticmethod
    def map_column_types(col_type: str):
        int_types = ("int", "integer")
        boolean_types = ("bool", "boolean")
        decimal_types = ("decimal", "money")
        result = col_type.lower()
        if result in int_types:
            return Integer
        if result.startswith(("str", "varchar", "nvarchar", "string")):
            match = re.search(r'\((\d+)\)', result)
            return String(int(match.group(1)))
        if result in boolean_types:
            return Boolean
        if result in decimal_types:
            return DECIMAL
        if result == "datetime":
            return DateTime


    @staticmethod
    def create_table_class(table_name: str, columns: list[TableModel]):

        table_attrs = {"__tablename__": table_name}
        for model in columns:
            table_attrs[model.col_name] = Column(TableFactory.map_column_types(model.col_type), primary_key=(model.col_name == "id"), nullable=model.nullable)

        table = type(table_name, (Base,), table_attrs)
        Base.metadata.create_all(engine)
        return table


    @staticmethod
    def drop_table(table_name: str):
        metadata = MetaData()
        try:
            table = Table(table_name, metadata, autoload_with=engine)
            table.drop(engine)
            return f"Table '{table_name}' has been dropped."
        except Exception as e:
            return f"Error dropping table '{table_name}': {e}"


    @staticmethod
    def get_all_tables():
        metadata = MetaData()
        metadata.reflect(bind=engine)
        return metadata.tables


    @staticmethod
    def get_table_columns(table_name):
        inspector = inspect(engine)
        return {column["name"]: column["type"] for column in inspector.get_columns(table_name)}


    @staticmethod
    def valid_col_type(col_type):
        if col_type in VALID_COL_TYPES:
            return True
        return False


if __name__ == "__main__":
    table = {
        TableModel("id", "int", False),
        TableModel("firstname", "String(50)", False),
        TableModel("surname", "str(50)", False),
        TableModel("married", "bool", False),
        TableModel("created_on", "datetime", False),
        TableModel("salary", "money", False)
    }

    TableFactory.create_table_class("complex_table_with_objects", table)
    # columns = TableFactory.get_table_columns("test")
    # for key, val in columns.items():
    #     print(f"{key} -> {val}")