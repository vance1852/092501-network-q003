"""依赖标准库的 JSON HTTP API。"""
from __future__ import annotations
import argparse,json,threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from urllib.parse import parse_qs,urlsplit
from .models import Reading,Segment
from .service import NetworkService
def _first(query,name):
    values=query.get(name)
    return values[0] if values else None
class Handler(BaseHTTPRequestHandler):
    service=NetworkService()
    lock=threading.Lock()
    def _send(self,status,payload):
        data=json.dumps(payload,ensure_ascii=False).encode(); self.send_response(status); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(data))); self.end_headers(); self.wfile.write(data)
    def _token(self):return self.headers.get("Authorization","").removeprefix("Bearer ")
    def do_GET(self):
        with self.lock: self._get()
    def _get(self):
        try:
            target=urlsplit(self.path); path=target.path; query=parse_qs(target.query)
            if path=="/health":return self._send(200,{"status":"ok","service":"urban-network"})
            if path.startswith("/segments/") and path.endswith("/readings"):
                return self._send(200,self.service.list_readings(self._token(),path.split("/")[2],start=_first(query,"start"),end=_first(query,"end"),cursor=_first(query,"cursor"),limit=_first(query,"limit") or 100))
            if path.startswith("/segments/") and path.endswith("/risk"):return self._send(200,self.service.risk_report(self._token(),path.split("/")[2]))
            if path.startswith("/audit/"):
                parts=path.split("/")
                if len(parts)==4 and parts[2] and parts[3]:return self._send(200,{"entity_type":parts[2],"entity_id":parts[3],"events":self.service.audit_events(self._token(),parts[2],parts[3])})
                return self._send(404,{"error":"not found"})
            if path.startswith("/segments/"):return self._send(200,self.service.segment(self._token(),path.split("/",2)[2]))
            return self._send(404,{"error":"not found"})
        except PermissionError as e:return self._send(403,{"error":str(e)})
        except Exception as e:return self._send(400,{"error":str(e)})
    def do_POST(self):
        with self.lock: self._post()
    def _post(self):
        try:
            body=json.loads(self.rfile.read(int(self.headers.get("Content-Length","0"))) or b"{}")
            if self.path=="/login":return self._send(200,{"token":self.service.auth.login(body["user_id"],body["password"])})
            token=self._token()
            if self.path=="/segments":return self._send(201,self.service.register_segment(token,Segment(body["segment_id"],body["district"],body["network_type"],body["length_m"],body["criticality"])))
            if self.path.startswith("/segments/") and self.path.endswith("/readings"):
                sid=self.path.split("/")[2]; r=Reading(body["reading_id"],sid,body["sensor_id"],body["pressure_kpa"],body["flow_lps"],body["acoustic_db"],body["observed_at"]); return self._send(201,self.service.ingest_reading(token,r))
            if self.path.startswith("/segments/") and self.path.endswith("/work-orders"):
                return self._send(201,self.service.create_work_order(token,self.path.split("/")[2],body["alert_id"],body["assignee"],body.get("priority",3)))
            return self._send(404,{"error":"not found"})
        except PermissionError as e:return self._send(403,{"error":str(e)})
        except Exception as e:return self._send(400,{"error":str(e)})
def main():
    p=argparse.ArgumentParser(); p.add_argument("--database",default=":memory:"); p.add_argument("--host",default="127.0.0.1"); p.add_argument("--port",type=int,default=8080); a=p.parse_args(); Handler.service=NetworkService(a.database); Handler.service.bootstrap(); ThreadingHTTPServer((a.host,a.port),Handler).serve_forever()
if __name__=="__main__":main()
