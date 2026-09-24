from unittest.mock import MagicMock, call

import pandas as pd
import pytest

from traffic.vcr_aadt_adt.writer import write_aadt_results


def _make_df():
    return pd.DataFrame({
        "LINKID": ["r1", "r2"],
        "VCR": [0.5, 0.6],
        "VOLUME": [100.0, 200.0],
        "CAPACITY": [200.0, 300.0],
        "CESA": [1.0, 2.0],
        "AADT": [150, 250],
    })


def test_deletes_existing_rows_for_year_semester():
    connection = MagicMock()
    cursor = connection.cursor.return_value

    write_aadt_results(connection, _make_df(), 2024, 2, None, "SMD.AADT")

    delete_call = cursor.execute.call_args_list[0]
    assert "DELETE FROM SMD.AADT" in delete_call.args[0]
    assert "YEAR = :1" in delete_call.args[0]
    assert "SEMESTER = :2" in delete_call.args[0]
    assert delete_call.args[1] == [2024, 2]


def test_narrows_delete_by_routes():
    connection = MagicMock()
    cursor = connection.cursor.return_value

    write_aadt_results(connection, _make_df(), 2024, 2, ["r1", "r2"], "SMD.AADT")

    delete_call = cursor.execute.call_args_list[0]
    assert "LINKID IN (:3, :4)" in delete_call.args[0]
    assert delete_call.args[1] == [2024, 2, "r1", "r2"]


def test_inserts_rows_with_year_and_semester():
    connection = MagicMock()
    cursor = connection.cursor.return_value

    row_count = write_aadt_results(connection, _make_df(), 2024, 2, None, "SMD.AADT")

    assert row_count == 2
    insert_call = cursor.executemany.call_args
    sql = insert_call.args[0]
    assert "INSERT INTO SMD.AADT" in sql
    assert "YEAR" in sql
    assert "SEMESTER" in sql
    values = insert_call.args[1]
    assert len(values) == 2
    assert values[0][-2:] == (2024, 2)


def test_commits_and_returns_row_count():
    connection = MagicMock()
    cursor = connection.cursor.return_value

    row_count = write_aadt_results(connection, _make_df(), 2024, 2, None, "SMD.AADT")

    connection.commit.assert_called_once()
    assert row_count == 2


def test_rolls_back_on_error():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.execute.side_effect = RuntimeError("db error")

    with pytest.raises(RuntimeError):
        write_aadt_results(connection, _make_df(), 2024, 2, None, "SMD.AADT")

    connection.rollback.assert_called_once()
    connection.commit.assert_not_called()


def test_linkid_uppercased():
    connection = MagicMock()
    cursor = connection.cursor.return_value
    df = _make_df()
    df["LINKID"] = ["R1", "r2"]

    write_aadt_results(connection, df, 2024, 2, None, "SMD.AADT")

    values = cursor.executemany.call_args.args[1]
    # LINKID is the first column and should be uppercased.
    assert values[0][0] == "R1"
    assert values[1][0] == "R2"


def test_none_routes_same_as_all():
    connection = MagicMock()
    cursor = connection.cursor.return_value

    write_aadt_results(connection, _make_df(), 2024, 2, None, "SMD.AADT")

    delete_call = cursor.execute.call_args_list[0]
    assert "LINKID" not in delete_call.args[0]
