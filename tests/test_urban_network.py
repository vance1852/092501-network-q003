import json,os,sqlite3,tempfile,threading,unittest,urllib.error,urllib.request
from http.server import ThreadingHTTPServer
from urllib.parse import urlencode
from urban_network import api as api_module
from urban_network.models import Reading,Segment,canonical_utc
from urban_network.risk import score_reading
from urban_network.service import NetworkService
from urban_network.storage import connect
class UrbanNetworkTests(unittest.TestCase):
    def setUp(self):
        self.s=NetworkService(); self.s.bootstrap(); self.t=self.s.auth.login("admin","network-admin"); self.s.register_segment(self.t,Segment("S1","east","drainage",100,4))
    def test_risk_and_idempotent_reading(self):
        r=Reading("R1","S1","sensor",120,250,90,"2026-01-01T00:00:00+00:00"); a=self.s.ingest_reading(self.t,r); b=self.s.ingest_reading(self.t,r); self.assertFalse(a["duplicate"]); self.assertTrue(b["duplicate"]); self.assertEqual(self.s.risk_report(self.t,"S1")["readings"],1)
    def test_work_order_and_allocation(self):
        r=self.s.ingest_reading(self.t,Reading("R2","S1","sensor",100,250,90,"2026-01-01T00:00:00+00:00")); o=self.s.create_work_order(self.t,"S1",r["alert_id"],"crew"); self.s.transition_work_order(self.t,o["work_order_id"],"assigned","crew accepted"); self.s.add_resource(self.t,"R1","pump","east",2); self.assertFalse(self.s.allocate(self.t,"R1",o["work_order_id"],1)["duplicate"]); self.assertEqual(self.s.resource(self.t,"R1")["available"],1)
    def test_risk_validation(self):
        with self.assertRaises(ValueError):score_reading(-1,1,1,2)
class TimezoneBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.s=NetworkService(); self.s.bootstrap(); self.t=self.s.auth.login("admin","network-admin"); self.s.register_segment(self.t,Segment("S1","east","gas",100,4))
    def ingest(self,reading_id,sensor,observed_at,pressure=120,flow=250,acoustic=90):
        return self.s.ingest_reading(self.t,Reading(reading_id,"S1",sensor,pressure,flow,acoustic,observed_at))
    def ids(self,page): return [r["reading_id"] for r in page["readings"]]
    def test_canonical_utc_collapses_representations(self):
        self.assertEqual(canonical_utc("2026-01-01T10:00:00+08:00"),"2026-01-01T02:00:00+00:00")
        self.assertEqual(canonical_utc("2026-01-01T02:00:00Z"),canonical_utc("2026-01-01T10:00:00+08:00"))
    def test_ingest_stores_canonical_utc(self):
        self.ingest("R1","gw-east","2026-01-01T10:00:00+08:00")
        stored=self.s.db.execute("SELECT observed_at FROM readings WHERE reading_id='R1'").fetchone()[0]
        self.assertEqual(stored,"2026-01-01T02:00:00+00:00")
    def test_true_time_order_and_latest_reading(self):
        self.ingest("R-early","gw-east","2026-01-01T10:00:00+08:00")  # 02:00 UTC
        self.ingest("R-late","gw-west","2026-01-01T09:00:00Z")       # 09:00 UTC
        page=self.s.list_readings(self.t,"S1")
        self.assertEqual(self.ids(page),["R-early","R-late"])
        self.assertIsNone(page["next_cursor"])
        report=self.s.risk_report(self.t,"S1")
        self.assertEqual(report["latest_reading"]["reading_id"],"R-late")
        self.assertEqual(report["latest_reading"]["observed_at"],"2026-01-01T09:00:00+00:00")
    def test_same_absolute_instant_follows_established_dedup(self):
        first=self.ingest("R1","gw-east","2026-01-01T10:00:00+08:00")
        replay=self.ingest("R1","gw-east","2026-01-01T02:00:00Z")
        self.assertFalse(first["duplicate"]); self.assertTrue(replay["duplicate"])
        self.assertEqual(self.s.risk_report(self.t,"S1")["readings"],1)
        self.assertEqual(len(self.s.db.execute("SELECT * FROM alerts").fetchall()),1)
        with self.assertRaises(sqlite3.IntegrityError):
            self.ingest("R2","gw-east","2026-01-01T02:00:00+00:00")
        self.assertEqual(self.s.risk_report(self.t,"S1")["readings"],1)
    def test_window_bounds_ignore_representation(self):
        self.ingest("R1","gw","2026-01-01T01:00:00Z")
        self.ingest("R2","gw","2026-01-01T10:00:00+08:00")  # 02:00 UTC
        self.ingest("R3","gw","2026-01-01T09:00:00Z")
        z_window=self.s.list_readings(self.t,"S1",start="2026-01-01T01:00:00Z",end="2026-01-01T02:00:00Z")
        offset_window=self.s.list_readings(self.t,"S1",start="2026-01-01T09:00:00+08:00",end="2026-01-01T10:00:00+08:00")
        self.assertEqual(self.ids(z_window),["R1","R2"]); self.assertEqual(self.ids(offset_window),["R1","R2"])
        with self.assertRaises(ValueError): self.s.list_readings(self.t,"S1",start="2026-01-02T00:00:00Z",end="2026-01-01T00:00:00Z")
        with self.assertRaises(ValueError): self.s.list_readings(self.t,"S1",start="not-a-time")
    def test_cursor_pagination_is_stable_across_representations(self):
        self.ingest("R1","gw","2026-01-01T01:00:00Z")
        self.ingest("R2","gw","2026-01-01T10:00:00+08:00")  # 02:00 UTC
        self.ingest("R3","gw","2026-01-01T09:00:00Z")
        first=self.s.list_readings(self.t,"S1",limit=2)
        self.assertEqual(self.ids(first),["R1","R2"]); self.assertTrue(first["next_cursor"])
        second=self.s.list_readings(self.t,"S1",cursor=first["next_cursor"])
        self.assertEqual(self.ids(second),["R3"]); self.assertIsNone(second["next_cursor"])
        mark,_,cursor_id=first["next_cursor"].partition("|")
        self.assertEqual(mark,"2026-01-01T02:00:00+00:00")
        shifted=self.s.list_readings(self.t,"S1",cursor="2026-01-01T10:00:00+08:00|"+cursor_id)
        self.assertEqual(self.ids(shifted),self.ids(second))
        with self.assertRaises(ValueError): self.s.list_readings(self.t,"S1",cursor="no-separator")
class LegacyMigrationTests(unittest.TestCase):
    def test_mixed_offset_history_migrates_without_touching_audit(self):
        with tempfile.TemporaryDirectory() as directory:
            path=os.path.join(directory,"legacy.sqlite3"); db=connect(path)
            db.execute("INSERT INTO segments VALUES(?,?,?,?,?,?,?,?)",("S1","east","gas",100,4,"normal","2026-01-01T00:00:00+00:00","2026-01-01T00:00:00+00:00"))
            db.execute("INSERT INTO readings VALUES(?,?,?,?,?,?,?)",("R-legacy-late","S1","gw-west",1,2,3,"2026-01-01T09:00:00Z"))
            db.execute("INSERT INTO readings VALUES(?,?,?,?,?,?,?)",("R-legacy-early","S1","gw-east",1,2,3,"2026-01-01T10:00:00+08:00"))
            db.execute("INSERT INTO readings VALUES(?,?,?,?,?,?,?)",("R-legacy-dup","S1","gw-east",1,2,3,"2026-01-01T02:00:00Z"))
            db.execute("INSERT INTO audit_events(entity_type,entity_id,action,actor,payload,created_at) VALUES(?,?,?,?,?,?)",("reading","R-legacy-early","ingested","gateway-east",json.dumps({"risk":{"severity":"low"}}),"2026-01-01T02:00:01+00:00"))
            db.commit(); before=[dict(r) for r in db.execute("SELECT * FROM audit_events ORDER BY event_id").fetchall()]; db.close()
            s=NetworkService(path); s.bootstrap(); t=s.auth.login("admin","network-admin")
            page=s.list_readings(t,"S1"); ids=[r["reading_id"] for r in page["readings"]]
            self.assertEqual(ids,["R-legacy-early","R-legacy-late"])
            self.assertEqual([r["observed_at"] for r in page["readings"]],["2026-01-01T02:00:00+00:00","2026-01-01T09:00:00+00:00"])
            self.assertEqual(s.risk_report(t,"S1")["latest_reading"]["reading_id"],"R-legacy-late")
            after=[dict(r) for r in s.db.execute("SELECT * FROM audit_events ORDER BY event_id").fetchall()]
            self.assertEqual(before,after)
            events=s.audit_events(t,"reading","R-legacy-early"); self.assertEqual(events[0]["actor"],"gateway-east")
            with self.assertRaises(sqlite3.IntegrityError):
                s.ingest_reading(t,Reading("R-new","S1","gw-east",1,2,3,"2026-01-01T02:00:00Z"))
            replay=s.ingest_reading(t,Reading("R-legacy-early","S1","gw-east",1,2,3,"2026-01-01T10:00:00+08:00"))
            self.assertTrue(replay["duplicate"])
            reopened=NetworkService(path); reopened.bootstrap(); rt=reopened.auth.login("admin","network-admin")
            self.assertEqual([r["reading_id"] for r in reopened.list_readings(rt,"S1")["readings"]],ids)
class _QuietHandler(api_module.Handler):
    def log_message(self,*args): pass
class UrbanNetworkApiTests(unittest.TestCase):
    def setUp(self):
        _QuietHandler.service=NetworkService(); _QuietHandler.service.bootstrap()
        self.server=ThreadingHTTPServer(("127.0.0.1",0),_QuietHandler)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread.start()
        self.base=f"http://127.0.0.1:{self.server.server_address[1]}"
    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join()
    def call(self,method,path,token=None,body=None):
        data=None if body is None else json.dumps(body).encode()
        request=urllib.request.Request(self.base+path,data=data,method=method)
        if token: request.add_header("Authorization","Bearer "+token)
        try:
            with urllib.request.urlopen(request) as response: return response.status,json.loads(response.read())
        except urllib.error.HTTPError as error: return error.code,json.loads(error.read())
    def test_api_observes_true_order_latest_and_audit(self):
        status,body=self.call("POST","/login",body={"user_id":"admin","password":"network-admin"}); token=body["token"]
        status,_=self.call("POST","/segments",token,{"segment_id":"S1","district":"east","network_type":"water","length_m":100,"criticality":4}); self.assertEqual(status,201)
        status,_=self.call("POST","/segments/S1/readings",token,{"reading_id":"R-early","sensor_id":"gw-east","pressure_kpa":120,"flow_lps":250,"acoustic_db":90,"observed_at":"2026-01-01T10:00:00+08:00"}); self.assertEqual(status,201)
        status,_=self.call("POST","/segments/S1/readings",token,{"reading_id":"R-late","sensor_id":"gw-west","pressure_kpa":120,"flow_lps":250,"acoustic_db":90,"observed_at":"2026-01-01T09:00:00Z"}); self.assertEqual(status,201)
        status,page=self.call("GET","/segments/S1/readings",token)
        self.assertEqual(status,200); self.assertEqual([r["reading_id"] for r in page["readings"]],["R-early","R-late"])
        self.assertEqual(page["readings"][0]["observed_at"],"2026-01-01T02:00:00+00:00")
        status,report=self.call("GET","/segments/S1/risk",token)
        self.assertEqual(report["latest_reading"]["reading_id"],"R-late")
        query=urlencode({"start":"2026-01-01T09:30:00+08:00","end":"2026-01-01T10:00:00+08:00"})
        status,window=self.call("GET","/segments/S1/readings?"+query,token)
        self.assertEqual([r["reading_id"] for r in window["readings"]],["R-early"])
        status,first=self.call("GET","/segments/S1/readings?"+urlencode({"limit":1}),token)
        self.assertEqual([r["reading_id"] for r in first["readings"]],["R-early"]); self.assertTrue(first["next_cursor"])
        status,second=self.call("GET","/segments/S1/readings?"+urlencode({"cursor":first["next_cursor"]}),token)
        self.assertEqual([r["reading_id"] for r in second["readings"]],["R-late"]); self.assertIsNone(second["next_cursor"])
        status,audit=self.call("GET","/audit/reading/R-early",token)
        self.assertEqual(status,200); self.assertEqual(audit["events"][0]["actor"],"admin"); self.assertEqual(audit["events"][0]["action"],"ingested")
if __name__=="__main__":
    unittest.main()
