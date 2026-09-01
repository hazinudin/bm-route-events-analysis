"""Export all SMD RNI, roughness (IRI), and PCI tables to Parquet."""

from __future__ import annotations

import argparse
import json
import re
from datetime import date, datetime
from pathlib import Path

import oracledb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml


TABLE_PATTERN = re.compile(r"^(?:RNI|ROUGHNESS|PCI)_[12]_[0-9]{4}$", re.IGNORECASE)


def arrow_type_for_oracle(type_code: object) -> pa.DataType:
    if type_code in (oracledb.DB_TYPE_NUMBER, oracledb.DB_TYPE_BINARY_FLOAT, oracledb.DB_TYPE_BINARY_DOUBLE):
        return pa.float64()
    if type_code in (oracledb.DB_TYPE_DATE, oracledb.DB_TYPE_TIMESTAMP):
        return pa.timestamp("ns")
    if type_code in (oracledb.DB_TYPE_BLOB, oracledb.DB_TYPE_RAW):
        return pa.binary()
    return pa.string()


def normalize_oracle_objects(frame: pd.DataFrame) -> pd.DataFrame:
    for column in frame.columns:
        values = frame[column].dropna()
        if not any(value.__class__.__name__ == "DbObject" for value in values):
            continue

        def serialize(value: object) -> str | None:
            if value is None:
                return None
            asdict = getattr(value, "asdict", None)
            if callable(asdict):
                try:
                    return json.dumps(asdict(), default=str, sort_keys=True)
                except Exception:
                    pass
            aslist = getattr(value, "aslist", None)
            if callable(aslist):
                try:
                    return json.dumps(aslist(), default=str)
                except Exception:
                    pass
            return str(value)

        frame[column] = frame[column].map(serialize)
    return frame


def load_profile(profile_path: Path, profile_name: str) -> dict:
    profiles = yaml.safe_load(profile_path.read_text())
    try:
        return profiles[profile_name]["outputs"][profiles[profile_name]["target"]]
    except KeyError as exc:
        raise RuntimeError(f"Invalid dbt profile: {profile_name}") from exc


def discover_tables(connection: oracledb.Connection, schema: str) -> list[str]:
    query = """
        SELECT table_name
        FROM all_tables
        WHERE owner = :owner
          AND REGEXP_LIKE(table_name, '^(RNI|ROUGHNESS|PCI)_[12]_[0-9]{4}$', 'i')
        ORDER BY table_name
    """
    with connection.cursor() as cursor:
        cursor.execute(query, owner=schema.upper())
        return [row[0] for row in cursor.fetchall()]


def export_table(
    connection: oracledb.Connection,
    schema: str,
    table_name: str,
    output_path: Path,
    chunk_size: int,
) -> int:
    qualified_name = f'"{schema.upper()}"."{table_name.upper()}"'
    row_count = 0
    writer: pq.ParquetWriter | None = None

    try:
        with connection.cursor() as cursor:
            cursor.arraysize = chunk_size
            cursor.execute(f"SELECT * FROM {qualified_name}")
            descriptions = cursor.description
            columns = [description[0] for description in descriptions]

            while rows := cursor.fetchmany(chunk_size):
                frame = pd.DataFrame.from_records(rows, columns=columns)
                frame = normalize_oracle_objects(frame)
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
                    # Oracle columns that are null in one chunk can otherwise be
                    # inferred as Arrow null and conflict with later string chunks.
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
        # Preserve an empty table's schema and make the output readable.
        empty = pa.Table.from_pandas(pd.DataFrame(columns=columns), preserve_index=False)
        pq.write_table(empty, output_path, compression="zstd")

    return row_count


def json_safe(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, default=Path("~/.dbt/profiles.yml").expanduser())
    parser.add_argument("--profile-name", default="events_analysis")
    parser.add_argument("--output-dir", type=Path, default=Path("~/Projects/bm-data-ds").expanduser())
    parser.add_argument("--chunk-size", type=int, default=25_000)
    args = parser.parse_args()

    config = load_profile(args.profile, args.profile_name)
    schema = config.get("schema", "SMD").upper()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    dsn = oracledb.makedsn(config["host"], config["port"], service_name=config["service"])
    connection = oracledb.connect(user=config["user"], password=config["password"], dsn=dsn)
    manifest: list[dict[str, object]] = []

    try:
        tables = discover_tables(connection, schema)
        if not tables:
            raise RuntimeError(f"No RNI, ROUGHNESS, or PCI tables found in {schema}")

        for table_name in tables:
            if not TABLE_PATTERN.fullmatch(table_name):
                continue
            output_path = args.output_dir / f"{table_name.lower()}.parquet"
            print(f"Exporting {schema}.{table_name} -> {output_path}")
            row_count = export_table(connection, schema, table_name, output_path, args.chunk_size)
            manifest.append({"schema": schema, "table": table_name, "file": output_path.name, "rows": row_count})
            print(f"  {row_count:,} rows")
    finally:
        connection.close()

    manifest_path = args.output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=json_safe) + "\n")
    print(f"Exported {len(manifest)} tables; manifest: {manifest_path}")


if __name__ == "__main__":
    main()
