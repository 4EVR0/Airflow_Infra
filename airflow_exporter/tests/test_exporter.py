import base64
import io
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from exporter import AirflowClient, Collector, Settings, Snapshot, escape_label, timestamp


class FakeClient:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, path, params):
        self.calls.append((path, params))
        key = "dags" if path == "/dags" else "task_instances" if path.endswith("/taskInstances") else "dag_runs"
        values = self.responses[path]
        offset = params["offset"]
        limit = params["limit"]
        return {key: values[offset : offset + limit], "total_entries": len(values)}


class BrokenClient:
    def get(self, path, params):
        raise ConnectionError("webserver unavailable")


class ExporterTests(unittest.TestCase):
    def setUp(self):
        self.settings = Settings("http://airflow-webserver:8080/api/v1", "viewer", "test-password")

    def test_active_and_last_completed_are_separate(self):
        run_id = "manual__2026/09/19+test"
        run_path = "/dags/oliveyoung_crawling/dagRuns"
        task_path = "/dags/oliveyoung_crawling/dagRuns/manual__2026%2F09%2F19%2Btest/taskInstances"
        client = FakeClient(
            {
                "/dags": [{"dag_id": "oliveyoung_crawling", "is_paused": False}],
                run_path: [
                    {"dag_run_id": run_id, "state": "running", "start_date": "2026-09-19T02:00:00Z"},
                    {
                        "dag_run_id": "scheduled__old",
                        "state": "success",
                        "start_date": "2026-09-16T02:00:00Z",
                        "end_date": "2026-09-17T02:00:00Z",
                    },
                ],
                task_path: [
                    {"task_id": "crawl", "state": "running", "map_index": -1, "start_date": "2026-09-19T02:10:00Z"},
                    {"task_id": "trigger_ec2", "state": "queued", "map_index": -1, "queued_when": "2026-09-19T02:20:00Z"},
                    {"task_id": "prepare", "state": "success", "map_index": -1},
                ],
            }
        )
        body = Collector(client, self.settings).collect()
        self.assertIn('airflow_observer_dag_last_completed_state{dag_id="oliveyoung_crawling",state="success"} 1', body)
        self.assertIn('airflow_observer_dag_active_run{dag_id="oliveyoung_crawling",run_id="manual__2026/09/19+test",state="running"} 1', body)
        self.assertIn('airflow_observer_task_active{dag_id="oliveyoung_crawling",map_index="-1",run_id="manual__2026/09/19+test",state="running",task_id="crawl"} 1', body)
        self.assertIn("airflow_observer_active_task_count 2", body)
        self.assertIn('airflow_observer_dag_last_completed_duration_seconds{dag_id="oliveyoung_crawling",state="success"} 86400', body)
        self.assertNotIn('task_id="prepare"', body)
        self.assertIn(task_path, [path for path, _ in client.calls])

    def test_failed_collection_never_reuses_old_task_data(self):
        client = FakeClient({"/dags": []})
        snapshot = Snapshot()
        snapshot.update(Collector(client, self.settings))
        self.assertIn(b"airflow_observer_collection_success 1", snapshot.render())
        self.assertIn(b"airflow_observer_dag_count 0", snapshot.render())
        with self.assertLogs("airflow_observer", level="ERROR"):
            snapshot.update(Collector(BrokenClient(), self.settings))
        body = snapshot.render()
        self.assertIn(b"airflow_observer_collection_success 0", body)
        self.assertNotIn(b"\nairflow_observer_dag_count ", body)
        self.assertGreater(snapshot.last_success, 0)

    def test_pagination_bound_fails_explicitly(self):
        client = FakeClient({"/dags": [{"dag_id": str(i)} for i in range(201)]})
        with self.assertRaisesRegex(ValueError, "configured bound"):
            Collector(client, self.settings).collect()

    def test_pagination_fetches_second_page(self):
        client = FakeClient({"/dags": [{"dag_id": str(i)} for i in range(101)]})
        result = Collector(client, self.settings).pages("/dags", "dags", 200)
        self.assertEqual(len(result), 101)
        self.assertEqual([params["offset"] for path, params in client.calls], [0, 100])

    def test_stale_snapshot_is_not_reported_as_current(self):
        snapshot = Snapshot(max_age_seconds=10)
        snapshot.success = 1
        snapshot.last_success = time.time() - 30
        snapshot.body = "airflow_observer_active_task_count 1\n"
        body = snapshot.render()
        self.assertIn(b"airflow_observer_collection_success 0", body)
        self.assertNotIn(b"\nairflow_observer_active_task_count 1", body)

    def test_timestamp_and_label_escaping(self):
        self.assertEqual(timestamp("2026-09-19T02:00:00Z"), timestamp("2026-09-19T11:00:00+09:00"))
        self.assertEqual(escape_label('a"b\\c\nd'), 'a\\"b\\\\c\\nd')
        with self.assertRaisesRegex(ValueError, "without timezone"):
            timestamp("2026-09-19T02:00:00")

    def test_rest_client_uses_get_and_basic_auth(self):
        seen = {}

        def fake_urlopen(request, timeout):
            seen["url"] = request.full_url
            seen["method"] = request.get_method()
            seen["authorization"] = request.get_header("Authorization")
            seen["timeout"] = timeout
            return io.BytesIO(b'{"dags":[],"total_entries":0}')

        with patch("exporter.urlopen", side_effect=fake_urlopen):
            result = AirflowClient(self.settings).get("/dags", {"limit": 100, "offset": 0})
        self.assertEqual(result["total_entries"], 0)
        self.assertEqual(seen["method"], "GET")
        self.assertEqual(seen["url"], "http://airflow-webserver:8080/api/v1/dags?limit=100&offset=0")
        self.assertEqual(seen["authorization"], "Basic " + base64.b64encode(b"viewer:test-password").decode())
        self.assertEqual(seen["timeout"], 5)


if __name__ == "__main__":
    unittest.main()
