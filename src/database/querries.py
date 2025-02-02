import re
from sqlalchemy import and_
from database.models import MetaData, engine, session, Table
from datetime import datetime
from decimal import Decimal

class QueryExecutor:
    """
    A class to execute SQL-like queries (SELECT, INSERT, UPDATE, DELETE) on a database using SQLAlchemy ORM.
    """

    @staticmethod
    def execute_query(user_query):
        """
        Executes the given SQL-like query and returns the result.

        Args:
            user_query (str): The query string to execute, which can be SELECT, INSERT, UPDATE, or DELETE.

        Returns:
            str or list: The result of the query, such as confirmation messages for INSERT/UPDATE/DELETE,
                         or a list of dictionaries for SELECT queries.
        """
        query = user_query.strip().lower()
        metadata = MetaData()

        match_where = re.search(r'\bwhere\b\s*(.*)', query, re.IGNORECASE)
        condition = None
        if match_where:
            condition = match_where.group(1).strip()

        if query.startswith("select"):
            return QueryExecutor._handle_select(query, metadata, condition)
        elif query.startswith("insert"):
            return QueryExecutor._handle_insert(query, metadata)
        elif query.startswith("update"):
            return QueryExecutor._handle_update(query, metadata, condition)
        elif query.startswith("delete"):
            return QueryExecutor._handle_delete(query, metadata, condition)

    @staticmethod
    def _handle_select(query, metadata, condition):
        """
        Handles the execution of SELECT queries.

        Args:
            query (str): The SELECT query to execute.
            metadata (MetaData): The metadata object for the database.
            condition (str): The WHERE condition for the query (if present).

        Returns:
            list: A list of dictionaries representing the result rows.
        """
        match = re.search(r'\bfrom\s+(\w+)', query, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            # Reflect the table dynamically
            table = Table(table_name, metadata, autoload_with=engine)

            # Build query using ORM
            if condition:
                # Parse condition into filters dynamically
                filters = QueryExecutor._parse_conditions(table, condition)
                rows = session.query(table).filter(and_(*filters)).all()
            else:
                rows = session.query(table).all()

            # Convert rows to dictionaries
            return [QueryExecutor._format_row(row, table) for row in rows]

    @staticmethod
    def _handle_insert(query, metadata):
        """
        Handles the execution of INSERT queries.

        Args:
            query (str): The INSERT query to execute.
            metadata (MetaData): The metadata object for the database.

        Returns:
            str: A message indicating the result of the insert operation.
        """
        match_with_columns = re.match(
            r"insert\s+into\s+(\w+)\s*\(([^)]+)\)\s*values\s*\(([^)]+)\)",
            query,
            re.IGNORECASE,
        )
        match_without_columns = re.match(
            r"insert\s+into\s+(\w+)\s*values\s*\(([^)]+)\)",
            query,
            re.IGNORECASE,
        )

        if match_with_columns:
            # INSERT with column names
            table_name, columns, values = match_with_columns.groups()
            columns = [col.strip() for col in columns.split(",")]
            values = [val.strip().strip("'") for val in values.split(",")]

        elif match_without_columns:
            # INSERT without column names
            table_name, values = match_without_columns.groups()
            table = Table(table_name, metadata, autoload_with=engine)
            columns = [col.name for col in table.columns]
            values = [val.strip().strip("'") for val in values.split(",")]

        else:
            return "Invalid INSERT query format."

        # Reflect the table dynamically
        table = Table(table_name, metadata, autoload_with=engine)

        # Validate column count matches values count
        if len(columns) != len(values):
            return f"Column count ({len(columns)}) does not match values count ({len(values)})."

        data = QueryExecutor._map_columns_to_values(columns, values, table)

        # Insert into the table
        insert_row = table.insert().values(**data)
        session.execute(insert_row)
        session.commit()
        return f"Row inserted into table '{table_name}'"

    @staticmethod
    def _handle_update(query, metadata, condition):
        """
        Handles the execution of UPDATE queries.

        Args:
            query (str): The UPDATE query to execute.
            metadata (MetaData): The metadata object for the database.
            condition (str): The WHERE condition for the query (if present).

        Returns:
            str: A message indicating the result of the update operation.
        """
        match = re.search(r'\bUPDATE\s+(\w+)\s+SET\s+(.*?)(\s+WHERE|$)', query, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            set_values = match.group(2)

            table = Table(table_name, metadata, autoload_with=engine)
            values = [val.strip().strip("'") for val in set_values.split(",")]
            data = {item.split("=")[0].strip(): item.split("=")[1].strip().strip("'") for item in values}
            update_row = table.update().values(**data)
            filters = QueryExecutor._parse_conditions(table, condition)
            update_row = update_row.where(and_(*filters))
            session.execute(update_row)
            session.commit()
            return f"Row updated for table '{table_name}'"
        return "Update query not ok."

    @staticmethod
    def _handle_delete(query, metadata, condition):
        """
        Handles the execution of DELETE queries.

        Args:
            query (str): The DELETE query to execute.
            metadata (MetaData): The metadata object for the database.
            condition (str): The WHERE condition for the query (if present).

        Returns:
            str: A message indicating the result of the delete operation.
        """
        match = re.search(r'\bfrom\s+(\w+)', query, re.IGNORECASE)
        if match:
            table_name = match.group(1)
            # Reflect the table dynamically
            table = Table(table_name, metadata, autoload_with=engine)
            # Build query using ORM
            if condition:
                # Parse condition into filters dynamically
                filters = QueryExecutor._parse_conditions(table, condition)
                deleted_row = table.delete().where(and_(*filters))
                session.execute(deleted_row)
                session.commit()
                return f"Rows deleted in table {table_name}"
            return "Missing where statement!"
        return "Delete query not ok."

    @staticmethod
    def _parse_conditions(table, condition_str):
        """
        Parses a WHERE condition into SQLAlchemy filters.

        Args:
            table (Table): The table to parse the conditions against.
            condition_str (str): The condition string (e.g., "column = value").

        Returns:
            list: A list of SQLAlchemy filter expressions.
        """
        filters = []
        for cond in condition_str.split(" and "):
            match = re.match(r'(\w+)\s*(=|>|<|>=|<=|!=)\s*(.+)', cond.strip())
            if match:
                column_name, operator, value = match.groups()
                column = getattr(table.c, column_name)

                # Map operator to SQLAlchemy expressions
                if operator == '=':
                    filters.append(column == value)
                elif operator == '>':
                    filters.append(column > value)
                elif operator == '<':
                    filters.append(column < value)
                elif operator == '>=':
                    filters.append(column >= value)
                elif operator == '<=':
                    filters.append(column <= value)
                elif operator == '!=':
                    filters.append(column != value)
        return filters

    @staticmethod
    def _format_row(row, table):
        """
        Formats a row into a dictionary with proper types for `datetime` and `Decimal`.

        Args:
            row (Row): A row from the query result.
            table (Table): The table object that defines the schema.

        Returns:
            dict: A dictionary where the keys are column names and the values are the corresponding data.
        """
        formatted_row = {}
        for col_name, col_value in zip(table.columns.keys(), row):
            if isinstance(col_value, datetime):
                formatted_row[col_name] = col_value.strftime('%Y-%m-%d %H:%M:%S')  # Convert to string
            elif isinstance(col_value, Decimal):
                formatted_row[col_name] = float(col_value)  # Convert to float
            else:
                formatted_row[col_name] = col_value
        return formatted_row

    @staticmethod
    def _map_columns_to_values(columns, values, table):
        """
        Maps column names to their corresponding values, converting types as necessary.

        Args:
            columns (list): A list of column names.
            values (list): A list of corresponding values.
            table (Table): The table object used for validation and type conversion.

        Returns:
            dict: A dictionary mapping column names to their respective values.
        """
        data = {}
        for col, val in zip(columns, values):
            column = getattr(table.c, col)
            if isinstance(val, str) and column.type.python_type == int:
                val = int(val)
            elif isinstance(val, str) and column.type.python_type == float:
                val = float(val)
            elif isinstance(val, str) and column.type.python_type == Decimal:
                val = Decimal(val)
            elif isinstance(val, str) and column.type.python_type == datetime:
                val = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
            data[col] = val
        return data


if __name__ == "__main__":
    query_result = QueryExecutor.execute_query(f"select * from complex_table")
    print(query_result)
