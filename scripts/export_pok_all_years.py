"""Export latest POK records per satker across all budget years."""

from __future__ import annotations

import argparse
from pathlib import Path

import oracledb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from export_smd_metrics import arrow_type_for_oracle, load_profile, normalize_oracle_objects


QUERY = """
WITH latest_pok_per_satker AS (
    SELECT r.*,
           RANK() OVER (
               PARTITION BY r.satker_code
               ORDER BY r.pok_status DESC
           ) AS rnk
    FROM POK.POK_JALAN_RAW r
)
SELECT LINKID, COMP_NAME, BUDGET_YEAR, START_IND, END_IND,
       X_STARTPOINT, Y_STARTPOINT, X_ENDPOINT, Y_ENDPOINT
FROM latest_pok_per_satker
WHERE rnk = 1
"""


def export_query(connection: oracledb.Connection, output_path: Path, chunk_size: int) -> int:
    row_count = 0
    writer: pq.ParquetWriter | None = None

    try:
        with connection.cursor() as cursor:
            cursor.arraysize = chunk_size
            cursor.execute(QUERY)
            descriptions = cursor.description
            columns = [description[0] for description in descriptions]

            while rows := cursor.fetchmany(chunk_size):
                frame = normalize_oracle_objects(pd.DataFrame.from_records(rows, columns=columns))
                if writer is None:
                    table = pa.Table.from_pandas(frame, preserve_index=False)
                    fields = [
                        pa.field(field.name, arrow_type_for_oracle(description[1]), nullable=True)
                        if pa.types.is_null(field.type)
                        else pa.field(field.name, pa.timestamp("us"), nullable=field.nullable)
                        if pa.types.is_timestamp(field.type)
                        else field
                        for field, description in zip(table.schema, descriptions)
                    ]
                    schema = pa.schema(fields)
                    table = pa.Table.from_pandas(frame, schema=schema, preserve_index=False, safe=False)
                    writer = pq.ParquetWriter(output_path, table.schema, compression="zstd")
                else:
                    table = pa.Table.from_pandas(
                        frame,
                        schema=writer.schema,
                        preserve_index=False,
                        safe=False,
                    )
                writer.write_table(table)
                row_count += len(frame)
    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        empty = pa.Table.from_pandas(pd.DataFrame(columns=columns), preserve_index=False)
        pq.write_table(empty, output_path, compression="zstd")

    return row_count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, default=Path("~/.dbt/profiles.yml").expanduser())
    parser.add_argument("--profile-name", default="events_analysis")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("~/Projects/bm-data-ds/pok_jalan_latest_all_years.parquet").expanduser(),
    )
    parser.add_argument("--chunk-size", type=int, default=25_000)
    args = parser.parse_args()

    config = load_profile(args.profile, args.profile_name)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    dsn = oracledb.makedsn(config["host"], config["port"], service_name=config["service"])
    connection = oracledb.connect(user=config["user"], password=config["password"], dsn=dsn)
    try:
        print(f"Exporting POK.POK_JALAN_RAW latest records -> {args.output}")
        row_count = export_query(connection, args.output, args.chunk_size)
    finally:
        connection.close()
    print(f"Exported {row_count:,} rows")


if __name__ == "__main__":
    main()
