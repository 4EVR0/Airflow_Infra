"""Read-only Airflow 2.9 REST observer for the host-local Alloy scrape."""

from __future__ import annotations

import base64
import json
import logging
import os
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

LOG = logging.getLogger("airflow_observer")
ACTIVE_RUN_STATES = {"running", "queued"}
ACTIVE_TASK_STATES = {"running", "queued"}
COMPLETED_RUN_STATES = {"success", "failed"}
PAGE_SIZE = 100
METRIC_HELP = {
    "airflow_observer_collection_success": "1 if the latest complete Airflow REST collection succeeded.",
    "airflow_observer_last_success_timestamp_seconds": "Unix time of the last successful collection.",
    "airflow_observer_last_attempt_timestamp_seconds": "Unix time of the last collection attempt.",
    "airflow_observer_collection_duration_seconds": "Duration of the latest collection attempt.",
    "airflow_observer_dag_known": "DAG visible through the Airflow API.",
    "airflow_observer_dag_last_completed_state": "State of the most recently completed DAG run.",
    "airflow_observer_dag_last_completed_end_timestamp_seconds": "End time of the most recently completed DAG run.",
    "airflow_observer_dag_last_completed_duration_seconds": "Duration of the most recently completed DAG run.",
    "airflow_observer_dag_active_run": "Currently queued or running DAG run.",
    "airflow_observer_dag_active_run_start_timestamp_seconds": "Start time of an active DAG run, when available.",
    "airflow_observer_task_active": "Currently queued or running task instance.",
    "airflow_observer_task_start_timestamp_seconds": "Start time of a running task instance, when available.",
    "airflow_observer_task_queued_timestamp_seconds": "Queue time of a queued task instance, when available.",
    "airflow_observer_dag_count": "Number of DAGs seen in the last successful collection.",
    "airflow_observer_active_run_count": "Number of queued or running DAG runs in the last successful collection.",
    "airflow_observer_active_task_count": "Number of queued or running task instances in the last successful collection.",
}


def timestamp(value: str | None) -> float | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"Airflow returned a timestamp without timezone: {value}")
    return parsed.timestamp()


def escape_label(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def sample(name: str, value: int | float, labels: dict[str, object] | None = None) -> str:
    suffix = ""
    if labels:
        suffix = "{" + ",".join(f'{key}="{escape_label(item)}"' for key, item in sorted(labels.items())) + "}"
    return f"{name}{suffix} {value}\n"


@dataclass(frozen=True)
class Settings:
    api_url: str
    username: str
    password: str
    interval_seconds: int = 60
    timeout_seconds: int = 5
    max_dags: int = 200
    max_runs_per_dag: int = 1000
    max_tasks_per_run: int = 1000
    listen_port: int = 9110

    @classmethod
    def from_env(cls) -> Settings:
        settings = cls(
            api_url=os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1").rstrip("/"),
            username=os.getenv("AIRFLOW_EXPORTER_USERNAME", ""),
            password=os.getenv("AIRFLOW_EXPORTER_PASSWORD", ""),
            interval_seconds=int(os.getenv("AIRFLOW_EXPORTER_INTERVAL_SECONDS", "60")),
            timeout_seconds=int(os.getenv("AIRFLOW_EXPORTER_TIMEOUT_SECONDS", "5")),
            listen_port=int(os.getenv("AIRFLOW_EXPORTER_PORT", "9110")),
        )
        if not settings.username or not settings.password:
            raise ValueError("AIRFLOW_EXPORTER_USERNAME and AIRFLOW_EXPORTER_PASSWORD are required")
        if not settings.api_url.startswith("http://") and not settings.api_url.startswith("https://"):
            raise ValueError("AIRFLOW_API_URL must be an HTTP(S) URL")
        if settings.interval_seconds < 10 or settings.timeout_seconds < 1:
            raise ValueError("exporter interval must be >=10s and timeout >=1s")
        return settings


class AirflowClient:
    def __init__(self, settings: Settings):
        self.base_url = settings.api_url
        self.timeout = settings.timeout_seconds
        credentials = f"{settings.username}:{settings.password}".encode()
        self.authorization = "Basic " + base64.b64encode(credentials).decode("ascii")

    def get(self, path: str, params: dict[str, object]) -> dict:
        url = f"{self.base_url}{path}?{urlencode(params)}"
        request = Request(url, headers={"Authorization": self.authorization, "Accept": "application/json"})
        with urlopen(request, timeout=self.timeout) as response:
            payload = json.load(response)
        if not isinstance(payload, dict):
            raise ValueError(f"Airflow returned a non-object response for {path}")
        return payload


class Collector:
    def __init__(self, client: AirflowClient, settings: Settings):
        self.client = client
        self.settings = settings

    def pages(self, path: str, key: str, max_items: int) -> list[dict]:
        items: list[dict] = []
        offset = 0
        while True:
            response = self.client.get(path, {"limit": PAGE_SIZE, "offset": offset})
            page = response.get(key)
            total = response.get("total_entries")
            if not isinstance(page, list) or not isinstance(total, int):
                raise ValueError(f"Airflow pagination contract changed for {path}")
            if len(items) + len(page) > max_items or total > max_items:
                raise ValueError(f"Airflow pagination exceeds configured bound for {path}: {total}")
            items.extend(page)
            offset += len(page)
            if offset >= total:
                return items
            if not page:
                raise ValueError(f"Airflow pagination stopped before total_entries for {path}")

    def collect(self) -> str:
        lines: list[str] = []
        dags = self.pages("/dags", "dags", self.settings.max_dags)
        active_run_count = 0
        active_task_count = 0

        for dag in dags:
            dag_id = dag["dag_id"]
            dag_path = f"/dags/{quote(dag_id, safe='')}"
            lines.append(sample("airflow_observer_dag_known", 1, {"dag_id": dag_id, "paused": str(bool(dag.get("is_paused", False))).lower()}))
            runs = self.pages(f"{dag_path}/dagRuns", "dag_runs", self.settings.max_runs_per_dag)
            completed = [run for run in runs if run.get("state") in COMPLETED_RUN_STATES and run.get("end_date")]
            if completed:
                last = max(completed, key=lambda run: timestamp(run["end_date"]) or 0)
                labels = {"dag_id": dag_id, "state": last["state"]}
                lines.append(sample("airflow_observer_dag_last_completed_state", 1, labels))
                end = timestamp(last["end_date"])
                start = timestamp(last.get("start_date"))
                if end is not None:
                    lines.append(sample("airflow_observer_dag_last_completed_end_timestamp_seconds", end, labels))
                if end is not None and start is not None:
                    lines.append(sample("airflow_observer_dag_last_completed_duration_seconds", max(0, end - start), labels))

            for run in runs:
                state = run.get("state")
                if state not in ACTIVE_RUN_STATES:
                    continue
                run_id = run["dag_run_id"]
                run_labels = {"dag_id": dag_id, "run_id": run_id, "state": state}
                lines.append(sample("airflow_observer_dag_active_run", 1, run_labels))
                started = timestamp(run.get("start_date"))
                if started is not None:
                    lines.append(sample("airflow_observer_dag_active_run_start_timestamp_seconds", started, run_labels))
                active_run_count += 1

                task_path = f"{dag_path}/dagRuns/{quote(run_id, safe='')}/taskInstances"
                tasks = self.pages(task_path, "task_instances", self.settings.max_tasks_per_run)
                for task in tasks:
                    task_state = task.get("state")
                    if task_state not in ACTIVE_TASK_STATES:
                        continue
                    labels = {
                        "dag_id": dag_id,
                        "run_id": run_id,
                        "task_id": task["task_id"],
                        "map_index": task.get("map_index", -1),
                        "state": task_state,
                    }
                    lines.append(sample("airflow_observer_task_active", 1, labels))
                    if task_state == "running":
                        started = timestamp(task.get("start_date"))
                        if started is not None:
                            lines.append(sample("airflow_observer_task_start_timestamp_seconds", started, labels))
                    else:
                        queued = timestamp(task.get("queued_when"))
                        if queued is not None:
                            lines.append(sample("airflow_observer_task_queued_timestamp_seconds", queued, labels))
                    active_task_count += 1

        lines.append(sample("airflow_observer_dag_count", len(dags)))
        lines.append(sample("airflow_observer_active_run_count", active_run_count))
        lines.append(sample("airflow_observer_active_task_count", active_task_count))
        return "".join(lines)


class Snapshot:
    def __init__(self, max_age_seconds: int = 180):
        self.lock = threading.Lock()
        self.max_age_seconds = max_age_seconds
        self.body = ""
        self.success = 0
        self.last_success = 0.0
        self.last_attempt = 0.0
        self.duration = 0.0

    def update(self, collector: Collector) -> None:
        started = time.time()
        try:
            body = collector.collect()
        except Exception:
            LOG.exception("Airflow REST collection failed")
            with self.lock:
                self.body = ""  # Never present stale run/task state as current.
                self.success = 0
                self.last_attempt = started
                self.duration = time.time() - started
        else:
            with self.lock:
                self.body = body
                self.success = 1
                self.last_success = started
                self.last_attempt = started
                self.duration = time.time() - started

    def render(self) -> bytes:
        with self.lock:
            fresh = bool(self.success and time.time() - self.last_success <= self.max_age_seconds)
            samples = [
                sample("airflow_observer_collection_success", int(fresh)),
                sample("airflow_observer_last_success_timestamp_seconds", self.last_success),
                sample("airflow_observer_last_attempt_timestamp_seconds", self.last_attempt),
                sample("airflow_observer_collection_duration_seconds", self.duration),
            ]
            if fresh:
                samples.extend(self.body.splitlines(keepends=True))
        by_name: dict[str, list[str]] = defaultdict(list)
        for line in samples:
            name = line.split("{", 1)[0].split(" ", 1)[0]
            by_name[name].append(line)
        lines = []
        for name, help_text in METRIC_HELP.items():
            lines.append(f"# HELP {name} {help_text}\n")
            lines.append(f"# TYPE {name} gauge\n")
            lines.extend(by_name.pop(name, []))
        if by_name:
            raise ValueError(f"missing metric metadata: {sorted(by_name)}")
        return "".join(lines).encode("utf-8")


def serve(snapshot: Snapshot, port: int) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/metrics":
                body = snapshot.render()
                status = 200
                content_type = "text/plain; version=0.0.4; charset=utf-8"
            elif self.path == "/healthz":
                with snapshot.lock:
                    fresh = snapshot.success and time.time() - snapshot.last_success <= snapshot.max_age_seconds
                    status = 200 if fresh else 503
                body = b"ok\n" if status == 200 else b"collection unavailable\n"
                content_type = "text/plain; charset=utf-8"
            else:
                status, body, content_type = 404, b"not found\n", "text/plain; charset=utf-8"
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: object) -> None:
            LOG.info(fmt, *args)

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    LOG.info("Airflow observer listening on :%d (container network only)", port)
    server.serve_forever()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    snapshot = Snapshot(max_age_seconds=3 * settings.interval_seconds)
    collector = Collector(AirflowClient(settings), settings)

    def poll() -> None:
        while True:
            snapshot.update(collector)
            time.sleep(settings.interval_seconds)

    threading.Thread(target=poll, daemon=True, name="airflow-poll").start()
    serve(snapshot, settings.listen_port)


if __name__ == "__main__":
    main()
