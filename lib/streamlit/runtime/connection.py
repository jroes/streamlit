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

import importlib

import streamlit as st
from streamlit.runtime.caching import singleton


def _retrieve_secrets(connection_name: str):
    return getattr(st.secrets.connection, connection_name)


def _retrieve_credentials(secrets):
    return secrets.credentials


def _import_adapter(connection_name, secret_values):
    module_path = secret_values.adapter.split(".")
    module_name = ".".join(module_path[0:-1])
    class_name = module_path[-1]

    module = importlib.import_module(module_name)
    Adapter = getattr(module, class_name)
    return Adapter


@singleton
def _get_connection(connection_name, **kwargs):
    secret_values = _retrieve_secrets(connection_name)
    Adapter = _import_adapter(connection_name, secret_values)
    adapter_instance = Adapter()
    if connection_name == "sqlite":
        # SQLLite takes a database name
        database_name = secret_values.database_name
        connection, cursor = adapter_instance.connect(database_name)
    if connection_name == "bigquery":
        # BigQuery takes connection credentials
        credentials = _retrieve_credentials(secret_values)
        connection, cursor = adapter_instance.connect(credentials)

    return adapter_instance, connection, cursor


@singleton
def dataframe_conversion_mapping():
    return {}


def register_data_type(from_type: str, to_type: str, conversion_func):
    mapping = dataframe_conversion_mapping()
    mapping[from_type] = mapping.get(from_type, {})
    mapping[from_type][to_type] = conversion_func


def _is_dataframe_conversion_available(data) -> bool:
    mapping = dataframe_conversion_mapping()
    if mapping.get(str(type(data)), None) is None:
        return False
    return True


def try_convert_to_dataframe(data_type, data):
    mapping = dataframe_conversion_mapping()
    conversion_func = mapping.get(str(data_type), {}).get(
        "pandas.core.frame.DataFrame", None
    )

    if conversion_func is None:
        return None
    else:
        return conversion_func(data)


def connection(connection_name: str, **kwargs):
    adapter, connection, cursor = _get_connection(connection_name, **kwargs)

    if not adapter.is_connected(connection):
        _get_connection.clear()
        dataframe_conversion_mapping.clear()
        adapter, connection, cursor = _get_connection(connection_name, **kwargs)

    return connection, cursor
