import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from datetime import timedelta, timezone
from http.server import ThreadingHTTPServer

from urban_network.api import Handler
from urban_network.clock import parse_utc
from urban_network.models import Reading, Segment
from urban_network.risk import score_reading
from urban_network.service import NetworkService
from urban_network.storage import connect, rows


class UrbanNetworkTests(unittest.TestCase):
    def setUp(self):
        self.s=NetworkService(); self.s.bootstrap(); self.t=self.s.auth.login("admin","network-admin"); self.s.register_segment(self.t,Segment("S1","east","drainage",100,4))
    def test_risk_and_idempotent_reading(self):
        r=Reading("R1","S1","sensor",120,250,90,"2026-01-01T00:00:00+00:00"); a=self.s.ingest_reading(self.t,r); b=self.s.ingest_reading(self.t,r); self.assertFalse(a["duplicate"]); self.assertTrue(b["duplicate"]); self.assertEqual(self.s.risk_report(self.t,"S1")["readings"],1)
    def test_work_order_and_allocation(self):
        r=self.s.ingest_reading(self.t,Reading("R2","S1","sensor",100,250,90,"2026-01-01T00:00:00+00:00")); o=self.s.create_work_order(self.t,"S1",r["alert_id"],"crew"); self.s.transition_work_order(self.t,o["work_order_id"],"assigned","crew accepted"); self.s.add_resource(self.t,"R1","pump","east",2); self.assertFalse(self.s.allocate(self.t,"R1",o["work_order_id"],1)["duplicate"]); self.assertEqual(self.s.resource(self.t,"R1")["available"],1)
    def test_risk_validation(self):
        with self.assertRaises(ValueError):score_reading(-1,1,1,2)


class TimestampBoundaryTests(unittest.TestCase):
    """两个网关分别上报 +08:00 与 Z 时间时的接收边界行为。"""

    def setUp(self):
        self.s = NetworkService()
        self.s.bootstrap()
        self.t = self.s.auth.login("admin", "network-admin")
        self.s.register_segment(self.t, Segment("S1", "east", "drainage", 100, 4))

    def ingest(self, reading_id, observed_at, sensor="gw-a"):
        return self.s.ingest_reading(self.t, Reading(reading_id, "S1", sensor, 350, 10, 10, observed_at))

    def test_ingest_normalizes_to_utc_and_dedups_same_instant(self):
        first = self.ingest("R10", "2026-09-21T10:00:00+08:00")
        second = self.ingest("R11", "2026-09-21T02:00:00Z")
        self.assertFalse(first["duplicate"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(second["reading_id"], "R10")
        stored = self.s.db.execute("SELECT observed_at FROM readings WHERE reading_id='R10'").fetchone()[0]
        self.assertEqual(stored, "2026-09-21T02:00:00Z")
        self.assertEqual(self.s.risk_report(self.t, "S1")["readings"], 1)

    def test_true_time_order_and_latest_reading(self):
        self.ingest("R20", "2026-09-21T13:00:00+08:00")  # 05:00Z
        self.ingest("R21", "2026-09-21T06:00:00Z")
        self.ingest("R22", "2026-09-21T04:00:00Z")
        page = self.s.list_readings(self.t, "S1")
        self.assertEqual([r["reading_id"] for r in page["readings"]], ["R22", "R20", "R21"])
        self.assertIsNone(page["next_cursor"])
        self.assertEqual(self.s.latest_reading(self.t, "S1")["reading_id"], "R21")
        self.assertEqual(self.s.risk_report(self.t, "S1")["latest_reading"]["reading_id"], "R21")

    def test_fractional_seconds_do_not_break_ordering(self):
        self.ingest("R30", "2026-09-21T02:00:00.500000Z")
        self.ingest("R31", "2026-09-21T02:00:00Z")
        page = self.s.list_readings(self.t, "S1")
        self.assertEqual([r["reading_id"] for r in page["readings"]], ["R31", "R30"])
        self.assertEqual(self.s.latest_reading(self.t, "S1")["reading_id"], "R30")

    def test_window_bounds_are_representation_independent(self):
        self.ingest("R40", "2026-09-21T04:00:00Z")
        self.ingest("R41", "2026-09-21T13:00:00+08:00")  # 05:00Z
        self.ingest("R42", "2026-09-21T06:00:00Z")
        zulu = self.s.list_readings(self.t, "S1", start="2026-09-21T04:30:00Z", end="2026-09-21T05:30:00Z")
        offset = self.s.list_readings(self.t, "S1", start="2026-09-21T12:30:00+08:00", end="2026-09-21T13:30:00+08:00")
        self.assertEqual(zulu, offset)
        self.assertEqual([r["reading_id"] for r in zulu["readings"]], ["R41"])
        with self.assertRaises(ValueError):
            self.s.list_readings(self.t, "S1", start="2026-09-21T06:00:00Z", end="2026-09-21T04:00:00Z")

    def test_cursor_pagination_is_stable_and_representation_independent(self):
        for rid, at in (("R50", "2026-09-21T04:00:00Z"), ("R51", "2026-09-21T13:00:00+08:00"),
                        ("R52", "2026-09-21T06:00:00Z"), ("R53", "2026-09-21T15:00:00+08:00"),
                        ("R54", "2026-09-21T08:00:00Z")):
            self.ingest(rid, at)
        seen, cursor = [], None
        while True:
            page = self.s.list_readings(self.t, "S1", limit=2, cursor=cursor)
            seen.extend(r["reading_id"] for r in page["readings"])
            cursor = page["next_cursor"]
            if cursor is None:
                break
        self.assertEqual(seen, ["R50", "R51", "R52", "R53", "R54"])
        first = self.s.list_readings(self.t, "S1", limit=2)
        cursor_time, cursor_id = first["next_cursor"].split("|", 1)
        shifted = parse_utc(cursor_time).astimezone(timezone(timedelta(hours=8))).isoformat()
        follow_z = self.s.list_readings(self.t, "S1", limit=2, cursor=first["next_cursor"])
        follow_offset = self.s.list_readings(self.t, "S1", limit=2, cursor=f"{shifted}|{cursor_id}")
        self.assertEqual(follow_z, follow_offset)
        self.assertEqual([r["reading_id"] for r in follow_z["readings"]], ["R52", "R53"])
        with self.assertRaises(ValueError):
            self.s.list_readings(self.t, "S1", cursor="not-a-cursor")


class LegacyMigrationTests(unittest.TestCase):
    """数据库里已有的带偏移记录在连接时迁移，审计来源保持不变。"""

    def test_migration_normalizes_offsets_dedups_and_preserves_audit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "legacy.sqlite3")
            db = connect(path)
            db.execute("INSERT INTO segments VALUES('S1','east','gas',100,4,'normal','2026-09-20T00:00:00+00:00','2026-09-20T00:00:00+00:00')")
            db.execute("INSERT INTO readings VALUES('L1','S1','gw-a',1,1,1,'2026-09-21T10:00:00+08:00')")
            db.execute("INSERT INTO readings VALUES('L2','S1','gw-a',2,2,2,'2026-09-21T02:00:00Z')")
            db.execute("INSERT INTO readings VALUES('L3','S1','gw-b',3,3,3,'2026-09-21T07:30:00+08:00')")
            db.execute("INSERT INTO audit_events(entity_type,entity_id,action,actor,payload,created_at) VALUES('reading','L1','ingested','op','{\"k\":1}','2026-09-21T02:00:00+00:00')")
            db.commit()
            before = rows(db, "SELECT * FROM audit_events ORDER BY event_id")
            db.close()

            migrated = connect(path)
            self.assertEqual(
                rows(migrated, "SELECT reading_id,observed_at FROM readings ORDER BY reading_id"),
                [{"reading_id": "L1", "observed_at": "2026-09-21T02:00:00Z"},
                 {"reading_id": "L3", "observed_at": "2026-09-20T23:30:00Z"}],
            )
            self.assertEqual(rows(migrated, "SELECT * FROM audit_events ORDER BY event_id"), before)
            migrated.close()

            service = NetworkService(path)
            service.bootstrap()
            token = service.auth.login("admin", "network-admin")
            replay = service.ingest_reading(token, Reading("L9", "S1", "gw-a", 1, 1, 1, "2026-09-21T02:00:00Z"))
            self.assertTrue(replay["duplicate"])
            self.assertEqual(replay["reading_id"], "L1")
            self.assertEqual(service.latest_reading(token, "S1")["reading_id"], "L1")
            service.db.close()


class UrbanApiTests(unittest.TestCase):
    """通过 HTTP API 观察真实时间顺序、最新读数与审计来源。"""

    def setUp(self):
        self.service = NetworkService()
        self.service.bootstrap()
        Handler.service = self.service
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.token = self.request("POST", "/login", {"user_id": "admin", "password": "network-admin"})["token"]

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def request(self, method, path, payload=None, token=None, params=None):
        if params:
            from urllib.parse import urlencode
            path = f"{path}?{urlencode(params)}"
        data = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(self.base + path, data=data, method=method)
        request.add_header("Content-Type", "application/json")
        if token:
            request.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(request) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return json.loads(exc.read())

    def test_api_observes_true_order_latest_and_audit(self):
        self.request("POST", "/segments", {"segment_id": "S9", "district": "east", "network_type": "gas", "length_m": 120, "criticality": 4}, self.token)
        self.request("POST", "/segments/S9/readings", {"reading_id": "A1", "sensor_id": "gw-a", "pressure_kpa": 350, "flow_lps": 10, "acoustic_db": 10, "observed_at": "2026-09-21T13:00:00+08:00"}, self.token)
        self.request("POST", "/segments/S9/readings", {"reading_id": "A2", "sensor_id": "gw-b", "pressure_kpa": 350, "flow_lps": 10, "acoustic_db": 10, "observed_at": "2026-09-21T06:30:00Z"}, self.token)
        duplicate = self.request("POST", "/segments/S9/readings", {"reading_id": "A3", "sensor_id": "gw-a", "pressure_kpa": 350, "flow_lps": 10, "acoustic_db": 10, "observed_at": "2026-09-21T05:00:00Z"}, self.token)
        self.assertTrue(duplicate["duplicate"])
        self.assertEqual(duplicate["reading_id"], "A1")

        page = self.request("GET", "/segments/S9/readings", token=self.token, params={"from": "2026-09-21T12:00:00+08:00", "to": "2026-09-21T06:30:00Z"})
        self.assertEqual([r["reading_id"] for r in page["readings"]], ["A1", "A2"])
        self.assertEqual(page["readings"][0]["observed_at"], "2026-09-21T05:00:00Z")

        latest = self.request("GET", "/segments/S9/readings/latest", token=self.token)
        self.assertEqual(latest["latest_reading"]["reading_id"], "A2")

        audit = self.request("GET", "/audit/reading/A1", token=self.token)
        self.assertEqual([event["action"] for event in audit["events"]], ["ingested"])
        self.assertEqual(audit["events"][0]["actor"], "admin")


if __name__ == "__main__":
    unittest.main()
