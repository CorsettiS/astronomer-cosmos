"""Tests for the databricks profile."""

from unittest.mock import patch

import pytest
from airflow.models.connection import Connection

from cosmos.profiles import get_automatic_profile_mapping
from cosmos.profiles.databricks import (
    DatabricksClientProfileMapping,
)


@pytest.fixture()
def mock_databricks_conn():  # type: ignore
    """
    Mocks and returns an Airflow Databricks connection.
    """
    conn = Connection(
        conn_id="my_databricks_connection",
        conn_type="databricks",
        host="https://my_host",
        login="my_client_id",
        password="my_client_secret",
        extra='{"http_path": "my_http_path"}',
    )

    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        yield conn


def test_connection_claiming() -> None:
    """
    Tests that the Databricks profile mapping claims the correct connection type.
    """
    # should only claim when:
    # - conn_type == databricks
    # and the following exist:
    # - schema
    # - host
    # - http_path
    # - client_id
    # - client_secret
    potential_values = {
        "conn_type": "databricks",
        "host": "my_host",
        "login": "my_client_id",
        "password": "my_client_secret",
        "extra": '{"http_path": "my_http_path"}',
    }

    # if we're missing any of the values, it shouldn't claim
    for key in potential_values:
        values = potential_values.copy()
        del values[key]
        conn = Connection(**values)  # type: ignore

        print("testing with", values)

        with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
            profile_mapping = DatabricksClientProfileMapping(conn, {"schema": "my_schema"})
            assert not profile_mapping.can_claim_connection()

    # also test when there's no schema
    conn = Connection(**potential_values)  # type: ignore
    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = DatabricksClientProfileMapping(conn, {})
        assert not profile_mapping.can_claim_connection()

    # if we have them all, it should claim
    conn = Connection(**potential_values)  # type: ignore
    with patch("airflow.hooks.base.BaseHook.get_connection", return_value=conn):
        profile_mapping = DatabricksClientProfileMapping(conn, {"schema": "my_schema"})
        assert profile_mapping.can_claim_connection()


def test_databricks_mapping_selected(
    mock_databricks_conn: Connection,
) -> None:
    """
    Tests that the correct profile mapping is selected.
    """
    profile_mapping = get_automatic_profile_mapping(
        mock_databricks_conn.conn_id,
        {"schema": "my_schema"},
    )
    assert isinstance(profile_mapping, DatabricksClientProfileMapping)
