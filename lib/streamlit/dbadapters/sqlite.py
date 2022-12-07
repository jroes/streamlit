# Copyright (c) Streamlit Inc. (2018-2022) Snowflake Inc. (2022)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sqlite3

import pandas as pd

from streamlit.runtime.connection import register_data_type


class SQLiteAdapter:
    def __init__(self):
        register_data_type(
            "pandas.core.frame.DataFrame", "sqlite3.Cursor", self.convert_to_dataframe
        )

    def connect(self, database_name: str) -> (sqlite3.Connection, sqlite3.Cursor):
        """
        Create an sqlite connection to the database provided in the
        current working directory or create it if it does not exist.
        """
        # returns the Connection object
        connection = sqlite3.connect(database_name)
        # returns the database cursor used to execute SQL statements & fetch results from SQL queries
        cursor = connection.cursor()
        return connection, cursor

    def is_connected(self, connection: sqlite3.Connection) -> bool:
        """Test if an sqlite connection is active"""
        try:
            connection.cursor()
            return True
        except Exception:
            return False

    def close(self, connection: sqlite3.Connection) -> None:
        """Close the sqlite connection"""
        connection.close()

    def convert_to_dataframe(self, cursor: sqlite3.Cursor) -> pd.DataFrame:
        cols = []
        for elt in cursor.description:
            cols.append(elt[0])

        return pd.DataFrame(cursor.fetchall(), columns=cols)
