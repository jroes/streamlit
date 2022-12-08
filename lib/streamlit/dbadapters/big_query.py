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

import google.cloud.bigquery as bigquery
import google.cloud.bigquery.dbapi as bq_dbapi
import pandas as pd
from google.oauth2 import service_account

from streamlit.runtime.connection import register_data_type

# Main docs: https://cloud.google.com/python/docs/reference/bigquery/latest
# DBAPI docs: https://googleapis.dev/python/bigquery/latest/dbapi.html
#               https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.dbapi
# Auth docs: https://googleapis.dev/python/google-auth/latest/user-guide.html


class BigQueryAdapter:
    def __init__(self):
        register_data_type(
            str(bq_dbapi.Cursor),
            "pandas.core.frame.DataFrame",
            self.convert_to_dataframe,
        )

    def connect(self, credentials) -> (bq_dbapi.Connection, bq_dbapi.Cursor):
        """
        Returns the BigQuery connection & cursor used to perform queries
        """
        creds = service_account.Credentials.from_service_account_info(credentials)
        Client = bigquery.Client(credentials=creds)

        connection = bq_dbapi.Connection(Client)
        cursor = connection.cursor()
        return connection, cursor

    def is_connected(self, connection: bq_dbapi.Connection) -> bool:
        """Test if an BigQuery connection is active"""
        try:
            # Per docs: This is a no-op, but for consistency raises an error if connection is closed
            # https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.dbapi.Connection#google_cloud_bigquery_dbapi_Connection_commit
            connection.commit()
            return True
        except Exception:
            return False

    def close(self, connection: bq_dbapi.Connection) -> None:
        """
        Close the BigQuery connection and any cursors created from it.
        """
        connection.close()

    def convert_to_dataframe(self, output) -> pd.DataFrame:
        # https://googleapis.dev/python/bigquery/latest/usage/pandas.html
        # TODO: Test if to_dataframe() call still works with dbapi
        return output.to_dataframe()
