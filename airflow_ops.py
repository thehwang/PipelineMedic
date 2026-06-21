#!/usr/bin/env python3
"""ETL ops helper for Cloud Composer 2 (Airflow 2.x) failure triage & remediation.

Designed to be driven by a Cursor Automation agent that receives an Airflow
failure message from a Slack ops channel. The agent calls these subcommands to:

  1. parse     -- turn the raw Slack failure text into structured identifiers
  2. logs      -- pull the task-instance logs via the Airflow REST API
  3. diagnose  -- classify the failure (transient vs. needs-human) from the logs
  4. rerun     -- clear the failed task so the scheduler reruns it (DRY-RUN by default)

Auth model (Composer 2):
  Composer 2's Airflow web server accepts a standard Google OAuth access token.
  We use Application Default Credentials (ADC) + an AuthorizedSession. The agent
  / runner just needs a service account (or user) with `roles/composer.user` on
  the environment. No IAP client-id dance is required (that was Composer 1).

Safety:
  - `rerun` defaults to DRY-RUN. Nothing is changed without --execute.
  - Acting on a *prod* host additionally requires --yes-prod.
  - `diagnose` only marks a failure auto-fixable when it is in the transient
    allowlist. Code/SQL/schema/data-quality errors are never auto-fixable.

The Airflow REST base URL is taken directly from the Log Url in the failure
message (it already contains the Composer web host), so no extra environment
lookup is needed in the common path. You can override with --airflow-uri.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from dataclasses import asdict, dataclass
from typing import Optional
from urllib.parse import parse_qs, quote, urlparse


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #
@dataclass
class Failure:
    dag_id: str
    task_id: str
    run_id: str
    execution_time: Optional[str]
    log_url: Optional[str]
    airflow_uri: Optional[str]  # scheme://host derived from log_url
    is_prod: bool

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


_DAG_RE = re.compile(r"Dag:\s*(?P<v>[^\n\r]+)", re.IGNORECASE)
_TASK_RE = re.compile(r"Task:\s*(?P<v>[^\n\r]+)", re.IGNORECASE)
_EXEC_RE = re.compile(r"Execution Time:\s*(?P<v>[^\n\r]+)", re.IGNORECASE)
_LOGURL_RE = re.compile(r"Log Url:\s*(?P<v>\S+)", re.IGNORECASE)


def parse_failure(text: str) -> Failure:
    """Parse the standard Composer failure-callback Slack message."""
    dag = _first(_DAG_RE, text)
    task = _first(_TASK_RE, text)
    exec_time = _first(_EXEC_RE, text)
    log_url = _first(_LOGURL_RE, text)

    run_id = None
    airflow_uri = None
    if log_url:
        parsed = urlparse(log_url)
        airflow_uri = f"{parsed.scheme}://{parsed.netloc}"
        qs = parse_qs(parsed.query)
        run_id = _one(qs.get("dag_run_id"))
        # Fall back to identifiers embedded in the log url if the text lines were
        # truncated by Slack.
        dag = dag or _path_after(parsed.path, "dags")
        task = task or _one(qs.get("task_id"))

    # run_id may also be reconstructed from execution time for scheduled runs.
    if not run_id and exec_time:
        run_id = f"scheduled__{exec_time.strip().replace(' ', 'T')}"

    is_prod = _looks_prod(text, airflow_uri)

    if not (dag and task and run_id):
        missing = [
            n
            for n, v in (("dag_id", dag), ("task_id", task), ("run_id", run_id))
            if not v
        ]
        raise ValueError(f"Could not parse required field(s): {', '.join(missing)}")

    return Failure(
        dag_id=dag.strip(),
        task_id=task.strip(),
        run_id=run_id.strip(),
        execution_time=exec_time.strip() if exec_time else None,
        log_url=log_url,
        airflow_uri=airflow_uri,
        is_prod=is_prod,
    )


def _first(rx: re.Pattern, text: str) -> Optional[str]:
    m = rx.search(text)
    return m.group("v").strip() if m else None


def _one(values) -> Optional[str]:
    return values[0] if values else None


def _path_after(path: str, segment: str) -> Optional[str]:
    parts = [p for p in path.split("/") if p]
    if segment in parts:
        i = parts.index(segment)
        if i + 1 < len(parts):
            return parts[i + 1]
    return None


def _looks_prod(text: str, airflow_uri: Optional[str]) -> bool:
    blob = f"{text}\n{airflow_uri or ''}".lower()
    return bool(re.search(r"\bprod\b", blob))


# --------------------------------------------------------------------------- #
# Airflow REST client (Composer 2)
# --------------------------------------------------------------------------- #
class AirflowClient:
    """Airflow 2.x REST client with pluggable auth.

    auth="adc"   -> Google Application Default Credentials (Cloud Composer 2).
    auth="basic" -> HTTP basic auth (local/self-hosted Airflow, demo env).
    """

    def __init__(
        self,
        airflow_uri: str,
        *,
        auth: str = "adc",
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.base = airflow_uri.rstrip("/")
        self.auth = auth
        self.username = username
        self.password = password
        self._session = None

    @classmethod
    def from_env(cls) -> "AirflowClient":
        """Build a client from PM_AIRFLOW_* env vars (defaults to local demo).

        PM_AIRFLOW_BASE_URL  default http://localhost:8080
        PM_AIRFLOW_AUTH      default basic   (basic|adc)
        PM_AIRFLOW_USERNAME  default admin
        PM_AIRFLOW_PASSWORD  default admin
        """
        import os

        return cls(
            os.environ.get("PM_AIRFLOW_BASE_URL", "http://localhost:8080"),
            auth=os.environ.get("PM_AIRFLOW_AUTH", "basic"),
            username=os.environ.get("PM_AIRFLOW_USERNAME", "admin"),
            password=os.environ.get("PM_AIRFLOW_PASSWORD", "admin"),
        )

    def _authed_session(self):
        if self._session is None:
            if self.auth == "basic":
                import requests

                s = requests.Session()
                s.auth = (self.username or "", self.password or "")
                self._session = s
            else:
                import google.auth
                from google.auth.transport.requests import AuthorizedSession

                creds, _ = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
                self._session = AuthorizedSession(creds)
        return self._session

    def _request(self, method: str, path: str, **kwargs):
        url = f"{self.base}/api/v1{path}"
        resp = self._authed_session().request(
            method, url, timeout=kwargs.pop("timeout", 60), **kwargs
        )
        if resp.status_code >= 400:
            raise RuntimeError(
                f"Airflow REST {method} {path} -> {resp.status_code}: {resp.text[:500]}"
            )
        return resp

    def task_instance(self, dag_id: str, run_id: str, task_id: str) -> dict:
        path = (
            f"/dags/{_enc(dag_id)}/dagRuns/{_enc(run_id)}"
            f"/taskInstances/{_enc(task_id)}"
        )
        return self._request("GET", path).json()

    def logs(
        self, dag_id: str, run_id: str, task_id: str, try_number: int = 1
    ) -> str:
        path = (
            f"/dags/{_enc(dag_id)}/dagRuns/{_enc(run_id)}"
            f"/taskInstances/{_enc(task_id)}/logs/{try_number}"
        )
        # full_content=true returns the raw text body instead of paginated json.
        resp = self._request(
            "GET", path, params={"full_content": "true"}, headers={"Accept": "text/plain"}
        )
        return resp.text

    def list_failed_task_instances(
        self, since_minutes: int, dag_ids: Optional[list] = None
    ) -> list:
        """Batch-list task instances that FAILED within the last `since_minutes`.

        Uses `~` wildcards for dag_id/dag_run_id and filters server-side on
        state + end_date so we only pull recently-finished failures.
        """
        import datetime as _dt

        end_gte = (
            _dt.datetime.now(_dt.timezone.utc)
            - _dt.timedelta(minutes=since_minutes)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        body = {"state": ["failed"], "end_date_gte": end_gte}
        if dag_ids:
            body["dag_ids"] = dag_ids
        path = "/dags/~/dagRuns/~/taskInstances/list"
        data = self._request("POST", path, json=body).json()
        return data.get("task_instances", [])

    def clear_task(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        *,
        dry_run: bool,
        include_downstream: bool = False,
    ) -> dict:
        path = f"/dags/{_enc(dag_id)}/clearTaskInstances"
        body = {
            "dry_run": dry_run,
            "dag_run_id": run_id,
            "task_ids": [task_id],
            "only_failed": True,
            "include_downstream": include_downstream,
            "include_upstream": False,
            "reset_dag_runs": True,
        }
        return self._request("POST", path, json=body).json()


def _enc(value: str) -> str:
    return quote(value, safe="")


def discover_airflow_uri(project: str, location: str, environment: str) -> str:
    """Resolve the Composer 2 Airflow web base via gcloud (no Slack msg needed)."""
    import subprocess

    out = subprocess.run(
        [
            "gcloud", "composer", "environments", "describe", environment,
            "--project", project, "--location", location,
            "--format", "value(config.airflowUri)",
        ],
        capture_output=True, text=True, check=True,
    )
    uri = out.stdout.strip()
    if not uri:
        raise RuntimeError(
            f"Empty airflowUri for {environment} ({project}/{location})."
        )
    return uri.rstrip("/")


# --------------------------------------------------------------------------- #
# Diagnosis
# --------------------------------------------------------------------------- #
# Ordered: first match wins. transient=True means safe to auto clear+rerun.
_SIGNATURES = [
    # category, transient, regex
    ("sensor_report_not_ready", True,
     r"report.*(not ready|pending|in[_ ]progress|processing)|status\s*[:=]\s*(pending|running)"),
    ("poke_timeout", True,
     r"(Sensor has timed out|poke|timed out waiting|reschedule).*", ),
    ("timeout", True, r"\b(timeout|timed out|deadline exceeded|read timed out)\b"),
    ("connection_reset", True,
     r"(connection reset|connection aborted|broken pipe|ECONNRESET|connection refused)"),
    ("rate_limited", True, r"\b(429|rate limit|quota exceeded|too many requests)\b"),
    ("upstream_5xx", True, r"\b(50[02348])\b.*(error|gateway|unavailable|service)"),
    ("worker_lost", True,
     r"(worker.*(lost|killed|SIGKILL)|task received SIGTERM|negsignal|zombie)"),
    ("oom", True, r"(out of memory|OOMKilled|MemoryError|Cannot allocate memory)"),

    # --- below here: NOT auto-fixable, needs a human / code change ---
    ("auth_permission", False,
     r"\b(403|401|permission denied|forbidden|access denied|invalid credentials)\b"),
    ("not_found", False, r"\b(404)\b|not found|no such (table|file|object|dataset)"),
    ("sql_error", False,
     r"(SQL compilation|syntax error|invalid query|BigQuery error|google\.api_core\.exceptions\.BadRequest)"),
    ("schema_mismatch", False,
     r"(schema (mismatch|mismatched|does not match)|column .* not found|cannot be null|incompatible type)"),
    ("data_quality", False,
     r"(data quality|dq[_ ]check|assertion|expectation failed|row count|validation failed)"),
    ("import_error", False, r"(ModuleNotFoundError|ImportError|cannot import name)"),
]


@dataclass
class Diagnosis:
    category: str
    transient: bool
    auto_fixable: bool
    summary: str
    recommended_action: str
    evidence: str

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


def diagnose(logs: str, is_prod: bool) -> Diagnosis:
    tail = logs[-8000:] if logs else ""
    for category, transient, pattern in _SIGNATURES:
        m = re.search(pattern, tail, re.IGNORECASE)
        if m:
            evidence = _context(tail, m.start(), m.end())
            return Diagnosis(
                category=category,
                transient=transient,
                auto_fixable=transient,
                summary=_SUMMARIES.get(category, category),
                recommended_action=(
                    "Clear the failed task to let the scheduler rerun it."
                    if transient
                    else "Do NOT auto-rerun. Open a fix PR / Jira and notify the owner."
                ),
                evidence=evidence,
            )
    return Diagnosis(
        category="unknown",
        transient=False,
        auto_fixable=False,
        summary="No known signature matched; manual review needed.",
        recommended_action="Escalate to a human; attach the log tail.",
        evidence=tail[-800:],
    )


_SUMMARIES = {
    "sensor_report_not_ready": "External report/dependency was not ready when the check ran.",
    "poke_timeout": "Sensor/poke timed out waiting for an external condition.",
    "timeout": "Operation timed out talking to an external system.",
    "connection_reset": "Transient network/connection error to a dependency.",
    "rate_limited": "Throttled / quota exceeded by an upstream service.",
    "upstream_5xx": "Upstream service returned a 5xx (transient outage).",
    "worker_lost": "Airflow worker was lost/killed mid-task.",
    "oom": "Task ran out of memory.",
    "auth_permission": "Authentication/permission error.",
    "not_found": "A required table/file/object was missing.",
    "sql_error": "SQL/query error in the task.",
    "schema_mismatch": "Schema/column/type mismatch.",
    "data_quality": "Data-quality / validation check failed.",
    "import_error": "Python import/dependency error.",
}


def _context(text: str, start: int, end: int, pad: int = 200) -> str:
    return text[max(0, start - pad) : min(len(text), end + pad)].strip()


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _read_text(args) -> str:
    if args.text:
        return args.text
    if args.file:
        with open(args.file, "r", encoding="utf-8") as fh:
            return fh.read()
    return sys.stdin.read()


def _resolve_failure(args) -> Failure:
    f = parse_failure(_read_text(args))
    if getattr(args, "airflow_uri", None):
        f.airflow_uri = args.airflow_uri.rstrip("/")
    if not f.airflow_uri:
        raise SystemExit(
            "No Airflow URI: message had no Log Url; pass --airflow-uri explicitly."
        )
    return f


def cmd_parse(args) -> int:
    print(parse_failure(_read_text(args)).to_json())
    return 0


def cmd_logs(args) -> int:
    f = _resolve_failure(args)
    client = AirflowClient(f.airflow_uri)
    print(client.logs(f.dag_id, f.run_id, f.task_id, args.try_number))
    return 0


def cmd_diagnose(args) -> int:
    f = _resolve_failure(args)
    client = AirflowClient(f.airflow_uri)
    logs = client.logs(f.dag_id, f.run_id, f.task_id, args.try_number)
    d = diagnose(logs, f.is_prod)
    out = {"failure": asdict(f), "diagnosis": asdict(d)}
    print(json.dumps(out, indent=2))
    return 0


def cmd_rerun(args) -> int:
    f = _resolve_failure(args)
    client = AirflowClient(f.airflow_uri)

    # Re-diagnose before acting unless explicitly skipped: never auto-rerun a
    # non-transient failure.
    if not args.force_category:
        logs = client.logs(f.dag_id, f.run_id, f.task_id, args.try_number)
        d = diagnose(logs, f.is_prod)
        if not d.auto_fixable:
            print(
                json.dumps(
                    {
                        "action": "refused",
                        "reason": f"Failure '{d.category}' is not in the auto-fix allowlist.",
                        "diagnosis": asdict(d),
                    },
                    indent=2,
                )
            )
            return 3

    if not args.execute:
        preview = client.clear_task(
            f.dag_id, f.run_id, f.task_id,
            dry_run=True, include_downstream=args.include_downstream,
        )
        print(json.dumps({"action": "dry_run", "would_clear": preview}, indent=2))
        return 0

    if f.is_prod and not args.yes_prod:
        raise SystemExit(
            "Refusing to act on a PROD environment without --yes-prod."
        )

    result = client.clear_task(
        f.dag_id, f.run_id, f.task_id,
        dry_run=False, include_downstream=args.include_downstream,
    )
    outcome = {"action": "cleared", "result": result}
    if args.wait_seconds > 0:
        outcome["final_state"] = _poll_state(
            client, f, deadline=time.time() + args.wait_seconds
        )
    print(json.dumps(outcome, indent=2))
    return 0


def _poll_state(client: AirflowClient, f: Failure, deadline: float) -> dict:
    last = None
    while time.time() < deadline:
        ti = client.task_instance(f.dag_id, f.run_id, f.task_id)
        last = ti.get("state")
        if last in ("success", "failed", "upstream_failed", "skipped"):
            break
        time.sleep(15)
    return {"state": last}


def _ti_key(ti: dict) -> str:
    return "::".join(
        str(ti.get(k, "")) for k in ("dag_id", "dag_run_id", "task_id", "try_number")
    )


def _load_seen(path: Optional[str]) -> set:
    if not path:
        return set()
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return set(json.load(fh))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def _save_seen(path: Optional[str], seen: set) -> None:
    if not path:
        return
    # Keep the state file bounded.
    trimmed = list(seen)[-5000:]
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(trimmed, fh)


def cmd_scan(args) -> int:
    if args.airflow_uri:
        uri = args.airflow_uri.rstrip("/")
    elif args.project and args.location and args.environment:
        uri = discover_airflow_uri(args.project, args.location, args.environment)
    else:
        raise SystemExit(
            "Provide --airflow-uri, or all of --project/--location/--environment."
        )

    client = AirflowClient(uri)
    dag_ids = args.dag_ids.split(",") if args.dag_ids else None
    failures = client.list_failed_task_instances(args.since_minutes, dag_ids)

    seen = _load_seen(args.state_file)
    is_prod = _looks_prod(uri, uri)
    new_items = []
    for ti in failures:
        key = _ti_key(ti)
        if key in seen:
            continue
        seen.add(key)
        item = {
            "dag_id": ti.get("dag_id"),
            "task_id": ti.get("task_id"),
            "run_id": ti.get("dag_run_id"),
            "try_number": ti.get("try_number"),
            "is_prod": is_prod,
            "airflow_uri": uri,
        }
        if args.diagnose:
            try:
                logs = client.logs(
                    ti["dag_id"], ti["dag_run_id"], ti["task_id"],
                    ti.get("try_number", 1),
                )
                item["diagnosis"] = asdict(diagnose(logs, is_prod))
            except RuntimeError as e:
                item["diagnosis_error"] = str(e)
        new_items.append(item)

    _save_seen(args.state_file, seen)
    print(json.dumps({"new_failures": new_items, "count": len(new_items)}, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="command", required=True)

    def add_input(sp):
        sp.add_argument("--text", help="Raw failure message text.")
        sp.add_argument("--file", help="Read failure message from a file.")
        sp.add_argument("--airflow-uri", help="Override Airflow web base URL.")

    sp = sub.add_parser("parse", help="Parse failure text -> JSON identifiers.")
    add_input(sp)
    sp.set_defaults(func=cmd_parse)

    sp = sub.add_parser("logs", help="Fetch task-instance logs.")
    add_input(sp)
    sp.add_argument("--try-number", type=int, default=1)
    sp.set_defaults(func=cmd_logs)

    sp = sub.add_parser("diagnose", help="Fetch logs and classify the failure.")
    add_input(sp)
    sp.add_argument("--try-number", type=int, default=1)
    sp.set_defaults(func=cmd_diagnose)

    sp = sub.add_parser("scan", help="Poll Composer for recent failed tasks (Slack-independent).")
    sp.add_argument("--airflow-uri", help="Airflow web base URL.")
    sp.add_argument("--project", help="GCP project (with --location/--environment).")
    sp.add_argument("--location", help="Composer region, e.g. us-east4.")
    sp.add_argument("--environment", help="Composer environment name.")
    sp.add_argument("--since-minutes", type=int, default=15,
                    help="Look back window for failures (default 15).")
    sp.add_argument("--dag-ids", help="Comma-separated DAG ids to limit the scan.")
    sp.add_argument("--diagnose", action="store_true",
                    help="Also pull logs and classify each new failure.")
    sp.add_argument("--state-file", help="JSON file to dedup already-seen failures.")
    sp.set_defaults(func=cmd_scan)

    sp = sub.add_parser("rerun", help="Clear the failed task so it reruns (DRY-RUN by default).")
    add_input(sp)
    sp.add_argument("--try-number", type=int, default=1)
    sp.add_argument("--execute", action="store_true", help="Actually clear (omit = dry-run).")
    sp.add_argument("--yes-prod", action="store_true", help="Required to act on prod.")
    sp.add_argument("--include-downstream", action="store_true")
    sp.add_argument("--force-category", action="store_true",
                    help="Skip the auto-fix allowlist gate (use with care).")
    sp.add_argument("--wait-seconds", type=int, default=0,
                    help="Poll for final task state up to N seconds after clearing.")
    sp.set_defaults(func=cmd_rerun)

    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.func(args)
    except (ValueError, RuntimeError) as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
