#!/usr/bin/env python3
"""Data-level evidence for ETL failure triage, via DuckDB + Iceberg on GCS.

Borrows the approach from `pq` (jq for Parquet): point DuckDB's httpfs at a
`gs://` path and read straight from cloud storage, no Spark/JVM. Where `pq`
targets plain parquet (`read_parquet`), this targets **Iceberg** tables with
`iceberg_scan(..., allow_moved_paths => true)` — same one-liner the team already
uses for ad-hoc checks.

Why: log-pattern diagnosis can't tell "report not ready" from "upstream wrote
zero rows" or "schema drifted". Probing the actual table gives data-grounded
evidence to attach to the diagnosis.

GCS auth mirrors pq's env contract:
    PQ_GCS_BEARER_TOKEN   (recommended: gcloud auth print-access-token)
  or PQ_GCS_HMAC_KEY + PQ_GCS_HMAC_SECRET   (long-lived, cron-friendly)
If neither is set we try `gcloud auth print-access-token` automatically.

Examples:
    # Iceberg table behind the failed DAG
    python data_probe.py --bucket my-data-lake-iceberg \
        --schema analytics --table events_daily_stg \
        --freshness-col event_date

    # Full URI form
    python data_probe.py --uri gs://my-data-lake-iceberg/analytics/events_daily_stg
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from typing import Optional


def gcs_bearer_token() -> Optional[str]:
    tok = os.environ.get("PQ_GCS_BEARER_TOKEN")
    if tok:
        return tok
    try:
        out = subprocess.run(
            ["gcloud", "auth", "print-access-token"],
            capture_output=True, text=True, check=True,
        )
        return out.stdout.strip() or None
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def _connect():
    import duckdb  # lazy: only needed when actually probing

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")

    hmac_key = os.environ.get("PQ_GCS_HMAC_KEY")
    hmac_secret = os.environ.get("PQ_GCS_HMAC_SECRET")
    if hmac_key and hmac_secret:
        con.execute(
            "CREATE OR REPLACE SECRET gcs_secret "
            "(TYPE gcs, KEY_ID ?, SECRET ?)",
            [hmac_key, hmac_secret],
        )
    else:
        token = gcs_bearer_token()
        if not token:
            raise RuntimeError(
                "No GCS credentials: set PQ_GCS_BEARER_TOKEN / PQ_GCS_HMAC_* "
                "or authenticate gcloud (gcloud auth print-access-token)."
            )
        con.execute(
            "CREATE OR REPLACE SECRET gcs_secret (TYPE gcs, BEARER_TOKEN ?)",
            [token],
        )
    return con


def build_uri(bucket: str, schema: str, table: str) -> str:
    return f"gs://{bucket}/{schema}/{table}"


@dataclass
class ProbeResult:
    uri: str
    reachable: bool
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    freshness_col: Optional[str] = None
    freshness_max: Optional[str] = None
    error: Optional[str] = None

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2, default=str)


def probe(uri: str, freshness_col: Optional[str] = None) -> ProbeResult:
    scan = f"iceberg_scan('{uri}', allow_moved_paths => true)"
    try:
        con = _connect()
    except RuntimeError as e:
        return ProbeResult(uri=uri, reachable=False, error=str(e))

    try:
        row_count = con.execute(f"SELECT count(*) FROM {scan}").fetchone()[0]
        col_count = len(con.execute(f"SELECT * FROM {scan} LIMIT 0").description)
        result = ProbeResult(
            uri=uri,
            reachable=True,
            row_count=int(row_count),
            column_count=col_count,
        )
        if freshness_col:
            fmax = con.execute(
                f"SELECT max({freshness_col}) FROM {scan}"
            ).fetchone()[0]
            result.freshness_col = freshness_col
            result.freshness_max = str(fmax) if fmax is not None else None
        return result
    except Exception as e:  # surface the DuckDB/Iceberg error as evidence
        return ProbeResult(uri=uri, reachable=False, error=str(e)[:500])
    finally:
        con.close()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--uri", help="Full gs:// iceberg table uri.")
    p.add_argument("--bucket", help="GCS bucket (with --schema/--table).")
    p.add_argument("--schema", help="Schema/dataset segment, e.g. analytics.")
    p.add_argument("--table", help="Table name.")
    p.add_argument("--freshness-col",
                   help="Column to take max() of as a freshness signal "
                        "(e.g. event_date / a date partition).")
    args = p.parse_args(argv)

    if args.uri:
        uri = args.uri.rstrip("/")
    elif args.bucket and args.schema and args.table:
        uri = build_uri(args.bucket, args.schema, args.table)
    else:
        raise SystemExit("Provide --uri, or all of --bucket/--schema/--table.")

    result = probe(uri, args.freshness_col)
    print(result.to_json())
    return 0 if result.reachable else 2


if __name__ == "__main__":
    raise SystemExit(main())
