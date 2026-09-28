"""DB-free unit tests for the deflection writeback layer (mocked cursor)."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.deflection import base as deflection_base
from src.deflection.fwd import FwdDeflection
from src.deflection.writer import write_deflection_results


def _make_df():
    return pd.DataFrame(
        {
            "LINKID": ["R1", "R2"],
            "STA": [0.0, 10.0],
            "SURVEY_DIREC": ["N", "N"],
            "FORCE": [40.0, 40.0],
            "FWD_D1": [100.0, 120.0],
            "FWD_D2": [80.0, 90.0],
            "ASPHALT_TEMP": [30.0, 30.0],
            "SURF_THICKNESS": [100.0, 100.0],
            "NORM_FWD_D1": [0.1, 0.12],
            "CORR_FWD_D1": [0.1, 0.12],
            "D0_D200": [0.02, 0.03],
        }
    )


def _fixed_ids(*ids):
    return lambda cursor, table, count: list(ids)[:count]


def _connection():
    connection = MagicMock()
    return connection, connection.cursor.return_value


def test_unknown_data_type_raises():
    connection, _ = _connection()
    with pytest.raises(ValueError, match="Unknown data_type"):
        write_deflection_results(connection, _make_df(), "XYZ", 2020)


def test_empty_df_returns_zero_without_db_access():
    connection, cursor = _connection()
    assert write_deflection_results(connection, pd.DataFrame(), "FWD", 2020) == 0
    connection.cursor.assert_not_called()
    cursor.execute.assert_not_called()
    cursor.executemany.assert_not_called()
    connection.commit.assert_not_called()


def test_none_df_returns_zero_without_db_access():
    connection, _ = _connection()
    assert write_deflection_results(connection, None, "FWD", 2020) == 0
    connection.cursor.assert_not_called()
    connection.commit.assert_not_called()


@pytest.mark.parametrize("routes", [None, "ALL"])
def test_delete_without_route_narrowing(routes):
    connection, cursor = _connection()
    write_deflection_results(
        connection, _make_df(), "FWD", 2020, routes, object_id_provider=_fixed_ids(1, 2)
    )

    delete_sql, delete_params = cursor.execute.call_args_list[0].args
    assert delete_sql == "DELETE FROM SMD.FWD WHERE YEAR = :1"
    assert "LINKID" not in delete_sql
    assert delete_params == [2020]


def test_delete_narrows_by_route_list():
    connection, cursor = _connection()
    write_deflection_results(
        connection,
        _make_df(),
        "FWD",
        2020,
        ["R1", "R2"],
        object_id_provider=_fixed_ids(1, 2),
    )

    delete_sql, delete_params = cursor.execute.call_args_list[0].args
    assert "LINKID IN (:2, :3)" in delete_sql
    assert delete_params == [2020, "R1", "R2"]


def test_delete_narrows_by_single_route():
    connection, cursor = _connection()
    write_deflection_results(
        connection, _make_df(), "FWD", 2020, "R1", object_id_provider=_fixed_ids(1, 2)
    )

    delete_sql, delete_params = cursor.execute.call_args_list[0].args
    assert "LINKID = :2" in delete_sql
    assert delete_params == [2020, "R1"]


def test_insert_shape_and_values():
    connection, cursor = _connection()
    write_deflection_results(
        connection,
        _make_df(),
        "FWD",
        2020,
        semester=2,
        object_id_provider=_fixed_ids(777, 888),
    )

    insert_sql, values = cursor.executemany.call_args.args
    assert "INSERT INTO SMD.FWD" in insert_sql
    assert "OBJECTID" in insert_sql
    assert "YEAR" in insert_sql
    assert "SEMESTER" in insert_sql
    assert len(values) == 2
    assert values[0][0] == 777
    assert values[1][0] == 888
    assert values[0][-2:] == (2020, 2)
    assert values[1][-2:] == (2020, 2)


def test_default_provider_allocates_object_id_per_row():
    connection, cursor = _connection()
    cursor.fetchone.side_effect = [(500,), (501,)]

    write_deflection_results(connection, _make_df(), "FWD", 2020)

    rowid_calls = [
        c for c in cursor.execute.call_args_list if "next_rowid" in c.args[0]
    ]
    assert len(rowid_calls) == 2
    assert rowid_calls[0].args[1] == ["FWD"]
    values = cursor.executemany.call_args.args[1]
    assert values[0][0] == 500
    assert values[1][0] == 501


def test_column_whitelist_excludes_unknown_columns():
    connection, cursor = _connection()
    df = _make_df()
    df["NOT_A_REAL_COLUMN"] = [1, 2]

    write_deflection_results(
        connection, df, "FWD", 2020, object_id_provider=_fixed_ids(1, 2)
    )

    insert_sql = cursor.executemany.call_args.args[0]
    assert "NOT_A_REAL_COLUMN" not in insert_sql


def test_commit_and_row_count():
    connection, _ = _connection()
    row_count = write_deflection_results(
        connection, _make_df(), "FWD", 2020, object_id_provider=_fixed_ids(1, 2)
    )
    assert row_count == 2
    connection.commit.assert_called_once()


def test_rollback_on_error():
    connection, cursor = _connection()
    cursor.executemany.side_effect = RuntimeError("db error")

    with pytest.raises(RuntimeError):
        write_deflection_results(
            connection, _make_df(), "FWD", 2020, object_id_provider=_fixed_ids(1, 2)
        )

    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()


def test_injected_object_id_provider_is_used():
    connection, cursor = _connection()
    seen = {}

    def provider(cursor_arg, table, count):
        seen["cursor"] = cursor_arg
        seen["table"] = table
        seen["count"] = count
        return [11, 22]

    write_deflection_results(
        connection, _make_df(), "FWD", 2020, object_id_provider=provider
    )

    assert seen == {"cursor": cursor, "table": "FWD", "count": 2}
    values = cursor.executemany.call_args.args[1]
    assert [row[0] for row in values] == [11, 22]


def _stub_temp_correction(self, lookup_table, source_col, corrected_col):
    self.sorted[corrected_col] = self.sorted[source_col]


@pytest.fixture
def stub_db(monkeypatch):
    monkeypatch.setattr(
        FwdDeflection, "_apply_temp_correction", _stub_temp_correction
    )


def test_save_results_requires_connection(stub_db):
    defl = FwdDeflection(_make_df())
    with pytest.raises(ValueError, match="requires a database connection"):
        defl.save_results(2020)


def test_save_results_requires_non_empty_sorted(stub_db):
    defl = FwdDeflection(_make_df().iloc[0:0], conn=MagicMock())
    with pytest.raises(ValueError, match="No calculation results to save"):
        defl.save_results(2020)


def test_save_results_delegates_to_writer(stub_db, monkeypatch):
    mock_writer = MagicMock(return_value=3)
    monkeypatch.setattr(deflection_base, "write_deflection_results", mock_writer)

    conn = MagicMock()
    defl = FwdDeflection(_make_df(), conn=conn)

    result = defl.save_results(2020, 2)

    assert result == 3
    connection = conn.raw_connection.return_value
    connection.close.assert_called_once()
    mock_writer.assert_called_once()
    args = mock_writer.call_args.args
    assert args[0] is connection
    assert args[2] == "FWD"
    assert args[3] == 2020
    assert args[4] == "ALL"
    assert args[5] == 2
