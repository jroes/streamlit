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


def _import_adapter(connection_name, secret_values):
    module_path = secret_values.adapter.split(".")
    module_name = ".".join(module_path[0:-1])
    class_name = module_path[-1]

    module = importlib.import_module(module_name)
    Adapter = getattr(module, class_name)
    # TODO: Refactor - Database name not used for BigQuery
    database_name = secret_values.database_name
    return Adapter, database_name


@singleton
def _get_connection(connection_name, **kwargs):
    secret_values = _retrieve_secrets(connection_name)
    Adapter, database_name = _import_adapter(connection_name, secret_values)
    adapter_instance = Adapter()
    # TODO: Refactor - Database name not used for BigQuery
    connection, cursor = adapter_instance.connect(database_name)
    return adapter_instance, connection, cursor


@singleton
def _conversion_mapping():
    return {}


def register_data_type(from_type: str, to_type: str, conversion_func):
    mapping = _conversion_mapping()
    mapping[to_type] = mapping.get(to_type, {})
    mapping[to_type][from_type] = conversion_func


def try_convert_to_dataframe(data_type, data):
    mapping = _conversion_mapping()
    st.write(mapping)
    conversion_func = mapping.get(data_type, {}).get(
        "pandas.core.frame.DataFrame", None
    )

    if conversion_func is None:
        st.error(
            f"""
            No conversion available for data type {data_type} to dataframe.
            """
        )
    else:
        return conversion_func(data)


def connection(connection_name: str, **kwargs):
    st.write("Inside the Connection API ✅")
    adapter, connection, cursor = _get_connection(connection_name, **kwargs)

    if not adapter.is_connected(connection):
        _get_connection.clear()
        _conversion_mapping.clear()
        adapter, connection, cursor = _get_connection(connection_name, **kwargs)

    return connection, cursor
