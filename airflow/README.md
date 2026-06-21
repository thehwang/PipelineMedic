# Local Airflow (demo environment)

A lightweight, self-contained Airflow used by PipelineMedic to produce **real**
failures with **real** logs and **real** clear/rerun behavior — no production
access required.

- Single container: `apache/airflow:2.10.4` running scheduler + webserver
- SQLite + SequentialExecutor (demo only, not production)
- REST API with basic auth — **admin / admin**
- UI / API: <http://localhost:8080>

## Bring it up

```bash
cd airflow
docker compose up -d
# first boot takes ~60s (provider imports); wait for health:
curl -s http://localhost:8080/health
```

## The three demo DAGs

| DAG | Failure | Auto-fixable? | Maps to signature |
|-----|---------|---------------|-------------------|
| `failing_timeout` | upstream read timeout | ✅ recovers on rerun | `timeout` |
| `failing_report_sensor` | report not ready (PENDING) | ✅ recovers on rerun | `sensor_report_not_ready` |
| `failing_bad_sql` | SQL syntax error | ❌ needs human fix | `sql_error` |

The two transient DAGs use a `/tmp` marker so the **first** run fails and a
**clear + rerun** succeeds — this demonstrates safe automatic remediation. The
SQL DAG always fails until the query is fixed, so the agent must escalate.

## Unpause + trigger (generate failures)

```bash
BASE=http://localhost:8080/api/v1
for dag in failing_timeout failing_report_sensor failing_bad_sql; do
  curl -s -u admin:admin -X PATCH "$BASE/dags/$dag" \
    -H 'Content-Type: application/json' -d '{"is_paused": false}'
  curl -s -u admin:admin -X POST "$BASE/dags/$dag/dagRuns" \
    -H 'Content-Type: application/json' -d '{}'
done
```

## Tear down

```bash
docker compose down        # keep image
docker compose down -v     # also remove volumes
```
