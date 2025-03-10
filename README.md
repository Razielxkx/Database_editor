# Database Editor

A simple web application for editing and managing database tables using Streamlit and SQLAlchemy. This tool allows users to interact with a MySQL database, view existing tables, and create new tables with custom columns.

## Features

- View all available tables in the database.
- View columns in a selected table.
- Create new tables with dynamic column names, types, and nullability.
- Dynamically add columns to a table during its creation.
- Support for multiple column types, including `int`, `string`, `bool`, `datetime`, and more.
- Execute simple select, insert, update, delete queries

## Technologies Used

- Python 3.x
- Streamlit
- SQLAlchemy
- MySQL Database

## Installation

### Prerequisites

- Python 3.x
- MySQL Server (locally or remotely accessible)
- Install the necessary Python packages:

```bash
pip install -r requirements.txt
