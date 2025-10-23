
# opcua_subscribe.py — OPCUASubscribe v1.2.2 (UI-friendly, lean Snowflake INSERTs + JWT support)
from __future__ import annotations
import asyncio, io, json, logging, traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from collections import deque
from threading import Lock, Thread, Event
from urllib.parse import urlparse, quote

from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.relationship import Relationship

try:
    from asyncua import Client, ua  # type: ignore
except Exception:
    Client = None; ua = None

try:
    from snowflake.connector import connect as sf_connect  # type: ignore
except Exception:
    sf_connect = None

try:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
except Exception:
    serialization = None; default_backend = None

__all__ = ["OPCUASubscribe", "create"]

class OPCUASubscribe(FlowFileSource):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileSource"]

    class ProcessorDetails:
        version = "1.3.1"
        description = ("Continuous OPC UA subscription with client/server deadband options. "
                       "Outputs JSON/CSV/XML or writes directly to Snowflake using lean connector INSERTs "
                       "(no pandas/pyarrow). Supports Snowflake key-pair (JWT) authentication.")
        tags = ["opcua", "subscribe", "deadband", "source", "openflow", "nifi", "snowflake", "jwt"]
        dependencies = ["asyncua>=1.0.0,<2.0.0", "cryptography>=41,<44", "snowflake-connector-python>=3.0"]

    REL_SUCCESS = Relationship("success", "Payload with changes, or Snowflake write summary.")
    REL_EMPTY   = Relationship("empty",   "No changes in the wait window (heartbeat JSON or 0-byte).")
    REL_FAILURE = Relationship("failure", "Text error payload.")

    def get_relationships(self): return [self.REL_SUCCESS, self.REL_EMPTY, self.REL_FAILURE]
    def getRelationships(self):  return self.get_relationships()

    def __init__(self, **kwargs):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        self._client: Optional[Any] = None
        self._seeded_nodes: set[str] = set()
        self._bg_thread: Optional[Thread] = None
        self._bg_loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_flag: bool = False
        self._connected: bool = False
        self._buffer_max: int = 50000
        self._buffer: deque = deque(maxlen=self._buffer_max)
        self._buf_lock = Lock()
        self._notify_event = Event()
        self._dropped_events: int = 0
        self._last_sent: Dict[str, Any] = {}
        self._active_signature: Optional[str] = None
        self._diag_last_start_err: Optional[str] = None
        self._diag_last_item_result: Dict[str, str] = {}
        self._diag_created_items: int = 0
        self._diag_fallback_items: int = 0
        self._last_effective_server_uri: Optional[str] = None

    def get_property_descriptors(self):
        try:
            V = StandardValidators
            non_empty = getattr(V, "NON_EMPTY_VALIDATOR", None)
            pos_int   = getattr(V, "POSITIVE_INTEGER_VALIDATOR", None)
            bool_val  = getattr(V, "BOOLEAN_VALIDATOR", None)

            self.P_ENDPOINT = PropertyDescriptor("OPC UA Endpoint","opc.tcp://host:port/Path",required=True,validators=[non_empty] if non_empty else [])
            self.P_SERVER_URI = PropertyDescriptor("Server URI Override","Exact ApplicationUri to send in CreateSession.ServerUri.",required=False,default_value="")
            self.P_SERVER_URI_MODE = PropertyDescriptor("Server URI Mode","auto|raw|encoded|empty",required=False,default_value="auto",allowable_values=["auto","raw","encoded","empty"])

            self.P_NODEIDS = PropertyDescriptor("NodeIds (CSV)","CSV of NodeIds (e.g., ns=2;s=Tag1,ns=2;s=Tag2).",required=True,validators=[non_empty] if non_empty else [])
            self.P_SAMPLING = PropertyDescriptor("Sampling Interval (ms)","Requested sampling interval.",required=False,default_value="250",validators=[pos_int] if pos_int else [])
            self.P_QUEUE = PropertyDescriptor("Monitored Item Queue Size","Server queue size per item (>=1).",required=False,default_value="256",validators=[pos_int] if pos_int else [])
            self.P_FILTER_MODE = PropertyDescriptor("Filter Mode","Where to apply filtering.",required=False,default_value="client",allowable_values=["client","server"])
            self.P_TRIGGER = PropertyDescriptor("Trigger","Server-side DataChange trigger (server mode).",required=False,default_value="StatusValue",allowable_values=["Status","Value","StatusValue","StatusValueTimestamp"])
            self.P_DEADBAND_ABS = PropertyDescriptor("Deadband Absolute","Absolute threshold.",required=False,default_value="")
            self.P_DEADBAND_PCT = PropertyDescriptor("Deadband Percent","Percent threshold (0-100).",required=False,default_value="")
            self.P_BLOCK_MS = PropertyDescriptor("Block Until Event (ms)","How long to wait for an event.",required=False,default_value="5000",validators=[pos_int] if pos_int else [])
            self.P_EMIT_INITIAL = PropertyDescriptor("Emit Initial On Connect","Emit one initial sample after connect.",required=False,default_value="true",validators=[bool_val] if bool_val else [])
            self.P_MAX_BUF = PropertyDescriptor("Max Client Buffer Events","Max events retained.",required=False,default_value="50000",validators=[pos_int] if pos_int else [])
            self.P_TIMEOUT = PropertyDescriptor("Timeout (sec)","Client operation timeout.",required=False,default_value="10",validators=[pos_int] if pos_int else [])
            self.P_USERNAME = PropertyDescriptor("Username","Optional",required=False,default_value="")
            self.P_PASSWORD = PropertyDescriptor("Password","Optional",required=False,default_value="",sensitive=True)

            self.P_OUTPUT_FMT = PropertyDescriptor("Output Format","json|csv|xml|snowflake",required=False,default_value="json",allowable_values=["json","csv","xml","snowflake"])

            self.P_CSV_DELIM = PropertyDescriptor("CSV Delimiter","Delimiter",required=False,default_value=",")
            self.P_CSV_HEADER = PropertyDescriptor("CSV Include Header","Include header",required=False,default_value="true",validators=[bool_val] if bool_val else [])

            self.P_XML_ROOT = PropertyDescriptor("XML Root Element","Root element name",required=False,default_value="opcuaEvents")
            self.P_XML_ROW  = PropertyDescriptor("XML Row Element","Row element name",required=False,default_value="event")

            self.P_SF_ACCOUNT = PropertyDescriptor("SF Account","Account (e.g., epckzel-squadron_us_east_1)",required=False,default_value="")
            self.P_SF_USER    = PropertyDescriptor("SF User","User",required=False,default_value="")
            self.P_SF_PASS    = PropertyDescriptor("SF Password","Password (blank if JWT)",required=False,default_value="",sensitive=True)
            self.P_SF_ROLE    = PropertyDescriptor("SF Role","Role",required=False,default_value="")
            self.P_SF_WH      = PropertyDescriptor("SF Warehouse","Warehouse",required=False,default_value="")
            self.P_SF_DB      = PropertyDescriptor("SF Database","Database",required=False,default_value="")
            self.P_SF_SCHEMA  = PropertyDescriptor("SF Schema","Schema",required=False,default_value="")
            self.P_SF_TABLE   = PropertyDescriptor("SF Table","Target table",required=False,default_value="")
            self.P_SF_AUTOCREATE = PropertyDescriptor("SF Auto-Create Table","Create table if missing",required=False,default_value="true",validators=[bool_val] if bool_val else [])
            self.P_SF_AUTH    = PropertyDescriptor("SF Authenticator","snowflake|snowflake_jwt",required=False,default_value="snowflake",allowable_values=["snowflake","snowflake_jwt"])
            self.P_SF_PRIVKEY = PropertyDescriptor("SF Private Key (PEM)","PEM for snowflake_jwt",required=False,default_value="",sensitive=True)
            self.P_SF_PRIVKEY_PASS = PropertyDescriptor("SF Private Key Passphrase","Passphrase (if encrypted)",required=False,default_value="",sensitive=True)

            return [
                self.P_ENDPOINT, self.P_SERVER_URI, self.P_SERVER_URI_MODE,
                self.P_NODEIDS, self.P_SAMPLING, self.P_QUEUE,
                self.P_FILTER_MODE, self.P_TRIGGER, self.P_DEADBAND_ABS, self.P_DEADBAND_PCT,
                self.P_BLOCK_MS, self.P_EMIT_INITIAL, self.P_MAX_BUF,
                self.P_TIMEOUT, self.P_USERNAME, self.P_PASSWORD,
                self.P_OUTPUT_FMT, self.P_CSV_DELIM, self.P_CSV_HEADER,
                self.P_XML_ROOT, self.P_XML_ROW,
                self.P_SF_ACCOUNT, self.P_SF_USER, self.P_SF_PASS, self.P_SF_ROLE,
                self.P_SF_WH, self.P_SF_DB, self.P_SF_SCHEMA, self.P_SF_TABLE, self.P_SF_AUTOCREATE,
                self.P_SF_AUTH, self.P_SF_PRIVKEY, self.P_SF_PRIVKEY_PASS
            ]
        except Exception:
            return [PropertyDescriptor("OPC UA Endpoint","Fallback",required=True)]

    def getPropertyDescriptors(self): return self.get_property_descriptors()

    def create(self, context):  return self.on_trigger(context)
    def onTrigger(self, context): return self.on_trigger(context)

    def on_trigger(self, context):
        try:
            if Client is None or ua is None:
                return self._route_failure("Missing asyncua dependency. Vendor it into the NAR.")

            endpoint  = self._get(context, "OPC UA Endpoint", "")
            node_ids  = [s.strip() for s in (self._get(context, "NodeIds (CSV)", "") or "").split(",") if s.strip()]
            sampling  = int(self._get(context, "Sampling Interval (ms)", "250") or "250")
            qsize     = int(self._get(context, "Monitored Item Queue Size", "256") or "256")
            filter_mode = (self._get(context, "Filter Mode", "client") or "client").lower()
            trigger_s   = (self._get(context, "Trigger", "StatusValue") or "StatusValue").strip()
            db_abs      = self._to_float_or_none(self._get(context, "Deadband Absolute", ""))
            db_pct      = self._to_float_or_none(self._get(context, "Deadband Percent", ""))
            block_ms  = int(self._get(context, "Block Until Event (ms)", "5000") or "5000")
            emit_init = (self._get(context, "Emit Initial On Connect", "true") or "true").lower() == "true"
            max_buf   = int(self._get(context, "Max Client Buffer Events", "50000") or "50000")
            timeout_s = int(self._get(context, "Timeout (sec)", "10") or "10")
            username  = self._get(context, "Username", "") or None
            password  = self._get(context, "Password", "") or None
            heartbeat = True  # default route to 'empty' with heartbeat JSON

            server_uri_override = (self._get(context, "Server URI Override", "") or "").strip()
            server_uri_mode = (self._get(context, "Server URI Mode", "auto") or "auto").strip().lower()

            out_fmt   = (self._get(context, "Output Format", "json") or "json").lower()
            csv_delim = (self._get(context, "CSV Delimiter", ",") or ",")
            csv_header = (self._get(context, "CSV Include Header", "true") or "true").lower() == "true"
            xml_root  = (self._get(context, "XML Root Element", "opcuaEvents") or "opcuaEvents")
            xml_row   = (self._get(context, "XML Row Element", "event") or "event")

            sf_account = self._normalize_account(self._get(context, "SF Account", "") or "")
            sf_user    = self._get(context, "SF User", "") or ""
            sf_pass    = self._get(context, "SF Password", "") or ""
            sf_role    = self._get(context, "SF Role", "") or ""
            sf_wh      = self._get(context, "SF Warehouse", "") or ""
            sf_db      = self._get(context, "SF Database", "") or ""
            sf_schema  = self._get(context, "SF Schema", "") or ""
            sf_table   = self._get(context, "SF Table", "") or ""
            sf_autocreate = (self._get(context, "SF Auto-Create Table", "true") or "true").lower() == "true"
            sf_auth    = (self._get(context, "SF Authenticator", "snowflake") or "snowflake").strip().lower()
            sf_priv_pem = self._get(context, "SF Private Key (PEM)", "") or ""
            sf_priv_pass = self._get(context, "SF Private Key Passphrase", "") or ""

            if not endpoint or not node_ids:
                return self._route_failure("OPC UA Endpoint and NodeIds (CSV) are required.")

            if max_buf != self._buffer_max and max_buf > 0:
                with self._buf_lock:
                    tail = list(self._buffer)[-max_buf:]
                    self._buffer = deque(tail, maxlen=max_buf)
                    self._buffer_max = max_buf

            signature = json.dumps({
                "endpoint": endpoint, "node_ids": node_ids, "sampling": sampling, "qsize": qsize,
                "filter_mode": filter_mode, "trigger": trigger_s, "db_abs": db_abs, "db_pct": db_pct,
                "timeout": timeout_s, "username": bool(username), "emit_init": emit_init,
                "server_uri_override": server_uri_override, "server_uri_mode": server_uri_mode
            }, sort_keys=True)

            if signature != self._active_signature or not self._connected:
                self._restart_background(endpoint=endpoint, node_ids=node_ids, sampling=sampling, qsize=qsize,
                    filter_mode=filter_mode, trigger_s=trigger_s, db_abs=db_abs, db_pct=db_pct,
                    timeout_s=timeout_s, username=username, password=password, emit_init=emit_init,
                    server_uri_override=server_uri_override, server_uri_mode=server_uri_mode)

            out_events: List[Dict[str, Any]] = []
            with self._buf_lock:
                while self._buffer:
                    out_events.append(self._buffer.popleft())

            if not out_events:
                self._notify_event.clear()
                self._notify_event.wait(timeout=max(0.0, block_ms / 1000.0))
                with self._buf_lock:
                    while self._buffer:
                        out_events.append(self._buffer.popleft())

            if not out_events and self._connected and emit_init and self._bg_loop and self._client is not None:
                try:
                    fut = asyncio.run_coroutine_threadsafe(self._seed_on_demand(node_ids), self._bg_loop)
                    try: fut.result(timeout=min(2.0, max(0.5, int(timeout_s) * 0.2)))
                    except Exception: pass
                    with self._buf_lock:
                        while self._buffer:
                            out_events.append(self._buffer.popleft())
                except Exception: pass

            attrs = {
                "opcua.connected": str(self._connected).lower(),
                "opcua.created_items": str(self._diag_created_items),
                "opcua.fallback_items": str(self._diag_fallback_items),
                "opcua.client_buffer_max": str(self._buffer_max),
                "opcua.client_buffer_dropped": str(self._dropped_events),
                "opcua.filter_mode": filter_mode,
                "opcua.endpoint.used": endpoint,
                "opcua.output.format": out_fmt,
            }
            self._dropped_events = 0

            eff_uri = getattr(self, "_last_effective_server_uri", None)
            if eff_uri is not None:
                attrs["opcua.serverUri.used"] = str(eff_uri)
            if self._diag_last_start_err: attrs["opcua.last_start_error"] = self._diag_last_start_err
            if self._diag_last_item_result: attrs["opcua.item_results.latest"] = json.dumps(self._diag_last_item_result, separators=(",", ":"))

            if not out_events:
                attrs["opcua.empty"] = "true"
                payload = {"mode":"subscribe","endpoint":endpoint,"timestamp_utc":self._iso_utc(),"events":[],"heartbeat":True}
                data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
                attrs["mime.type"] = "application/json"
                return FlowFileSourceResult("empty", attrs, data)

            attrs["opcua.empty"] = "false"
            if out_fmt == "json":
                payload = {"mode":"subscribe","endpoint":endpoint,"timestamp_utc":self._iso_utc(),"events":out_events}
                data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
                attrs["mime.type"] = "application/json"
                return FlowFileSourceResult("success", attrs, data)

            elif out_fmt == "csv":
                cols = ["nodeId","value","dataType","status","statusName","statusCode","sourceTimestamp","serverTimestamp"]
                buf = io.StringIO()
                if csv_header: buf.write(csv_delim.join(cols) + "\n")
                for e in out_events:
                    row = [self._csv_cell(e.get(c), csv_delim) for c in cols]
                    buf.write(csv_delim.join(row) + "\n")
                data = buf.getvalue().encode("utf-8")
                attrs["mime.type"] = "text/csv"; attrs["csv.delimiter"] = csv_delim; attrs["csv.header"] = str(csv_header).lower()
                return FlowFileSourceResult("success", attrs, data)

            elif out_fmt == "xml":
                def esc(v):
                    s = "" if v is None else str(v)
                    return (s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"',"&quot;").replace("'","&apos;"))
                ts = self._iso_utc()
                parts = [f'<?xml version="1.0" encoding="UTF-8"?>', f'<{xml_root} endpoint="{esc(endpoint)}" timestamp_utc="{esc(ts)}">']
                for e in out_events:
                    parts.append(f'  <{xml_row}>')
                    for k in ["nodeId","value","dataType","status","statusName","statusCode","sourceTimestamp","serverTimestamp"]:
                        parts.append(f'    <{k}>{esc(e.get(k))}</{k}>')
                    parts.append(f'  </{xml_row}>')
                parts.append(f'</{xml_root}>')
                data = ("\n".join(parts)).encode("utf-8"); attrs["mime.type"] = "application/xml"
                return FlowFileSourceResult("success", attrs, data)

            elif out_fmt == "snowflake":
                if sf_connect is None:
                    return self._route_failure("Snowflake sink selected but snowflake-connector-python is not bundled.")
                if not (sf_account and sf_user and sf_wh and sf_db and sf_schema and sf_table):
                    return self._route_failure("Missing snowflake properties (account,user,warehouse,database,schema,table).")
                if (sf_auth == "snowflake") and (not sf_pass):
                    return self._route_failure("SF Password is required when SF Authenticator = snowflake.")
                if sf_auth == "snowflake_jwt":
                    if serialization is None:
                        return self._route_failure("cryptography is required for snowflake_jwt (not bundled).")
                    if not sf_priv_pem.strip():
                        return self._route_failure("SF Private Key (PEM) is required for snowflake_jwt.")

                rows_written = self._write_to_snowflake_direct(
                    events=out_events, endpoint=endpoint,
                    account=sf_account, user=sf_user, password=sf_pass, role=sf_role,
                    warehouse=sf_wh, database=sf_db, schema=sf_schema, table=sf_table,
                    autocreate=sf_autocreate, authenticator=sf_auth,
                    private_key_pem=sf_priv_pem, private_key_pass=sf_priv_pass
                )
                summary = {"mode":"subscribe","endpoint":endpoint,"timestamp_utc":self._iso_utc(),
                           "events_written":rows_written,"table":f"{sf_db}.{sf_schema}.{sf_table}"}
                attrs["mime.type"] = "application/json"
                attrs["snowflake.rows_written"] = str(rows_written)
                attrs["snowflake.table"] = f"{sf_db}.{sf_schema}.{sf_table}"
                return FlowFileSourceResult("success", attrs, json.dumps(summary).encode("utf-8"))

            else:
                return self._route_failure(f"Unsupported Output Format: {out_fmt}")

        except Exception as e:
            return self._route_failure(f"{type(e).__name__}: {e}\n{traceback.format_exc()}")

    def onStopped(self): self._stop_background(True)
    def onStop(self):    self._stop_background(True)
    def onUnscheduled(self): self._stop_background(True)
    def stop(self):      self._stop_background(True)

    def _restart_background(self, *, endpoint: str, node_ids: List[str], sampling: int, qsize: int,
                            filter_mode: str, trigger_s: str, db_abs: Optional[float], db_pct: Optional[float],
                            timeout_s: int, username: Optional[str], password: Optional[str], emit_init: bool,
                            server_uri_override: str, server_uri_mode: str):
        self._stop_background(True); self._stop_flag = False; self._connected = False
        self._seeded_nodes.clear()
        self._active_signature = json.dumps({
            "endpoint": endpoint, "node_ids": node_ids, "sampling": sampling, "qsize": qsize,
            "filter_mode": filter_mode, "trigger": trigger_s, "db_abs": db_abs, "db_pct": db_pct,
            "timeout": timeout_s, "username": bool(username), "emit_init": emit_init,
            "server_uri_override": server_uri_override, "server_uri_mode": server_uri_mode
        }, sort_keys=True)

        def runner():
            try:
                loop = asyncio.new_event_loop(); self._bg_loop = loop
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self._subscriber_loop(
                    endpoint=endpoint, node_ids=node_ids, sampling=sampling, qsize=qsize,
                    filter_mode=filter_mode, trigger_s=trigger_s, db_abs=db_abs, db_pct=db_pct,
                    timeout_s=timeout_s, username=username, password=password, emit_init=emit_init,
                    server_uri_override=server_uri_override, server_uri_mode=server_uri_mode
                ))
            except Exception as e:
                self._diag_last_start_err = f"{type(e).__name__}: {e}"
            finally:
                try:
                    if self._bg_loop and not self._bg_loop.is_closed(): self._bg_loop.close()
                except Exception: pass
                self._bg_loop = None

        self._bg_thread = Thread(target=runner, name=f"{self.__class__.__name__}-subscriber", daemon=True)
        self._bg_thread.start()

    def _stop_background(self, join: bool):
        try:
            self._stop_flag = True
            if self._bg_loop:
                try: self._bg_loop.call_soon_threadsafe(lambda: None)
                except Exception: pass
        except Exception: pass
        if join and self._bg_thread and self._bg_thread.is_alive():
            try: self._bg_thread.join(timeout=2.0)
            except Exception: pass
        self._bg_thread = None; self._connected = False

    async def _subscriber_loop(self, *, endpoint: str, node_ids: List[str], sampling: int, qsize: int,
                               filter_mode: str, trigger_s: str, db_abs: Optional[float], db_pct: Optional[float],
                               timeout_s: int, username: Optional[str], password: Optional[str], emit_init: bool,
                               server_uri_override: str, server_uri_mode: str):
        if Client is None or ua is None: return
        override_raw = (server_uri_override or "").strip()

        async def discover_app_uri() -> str:
            try:
                client = Client(url=endpoint, timeout=timeout_s)
                try:
                    await client.connect(); eps = await client.get_endpoints()
                finally:
                    try: await client.disconnect()
                    except Exception: pass
                best = ""
                for ep in eps:
                    server_desc = getattr(ep, "Server", getattr(ep, "server", None))
                    app_uri = (getattr(server_desc, "ApplicationUri", getattr(server_desc, "application_uri", None)) if server_desc else None)
                    if app_uri and not best: best = str(app_uri)
                return best
            except Exception: return ""

        def encode_if_needed(v: str) -> str: return self._sanitize_uri(v)
        async def pick_auto() -> str: return (override_raw or (await discover_app_uri()) or "").strip()

        if server_uri_mode == "empty": chosen = ""
        elif server_uri_mode == "raw": chosen = override_raw
        elif server_uri_mode == "encoded": chosen = encode_if_needed(override_raw)
        else: chosen = await pick_auto()

        async def try_connect(with_uri: str):
            client = Client(url=endpoint, timeout=timeout_s)
            self._apply_server_uri_soft(client, with_uri); self._patch_create_session_hard(client, with_uri)
            self._last_effective_server_uri = with_uri
            if username:
                try:
                    client.set_user(username)
                    if password: client.set_password(password)
                except Exception: pass
            self._client = client
            try:
                async with client:
                    self._connected = True
                    self._diag_created_items = 0; self._diag_fallback_items = 0; self._diag_last_item_result = {}
                    sub = await client.create_subscription(sampling, _Callback(self, filter_mode, db_abs, db_pct))
                    try:
                        created_any = await self._create_items(client, sub, node_ids, sampling, qsize, filter_mode, trigger_s, db_abs, db_pct)
                        if not created_any: return False
                        if emit_init: await self._emit_initial_baseline(client, node_ids)
                        while not self._stop_flag: await asyncio.sleep(0.2)
                    finally:
                        try: await sub.delete(); self._client = None
                        except Exception: pass
                return True
            except Exception as e:
                s = str(e); self._connected = False; self._client = None
                if "BadServerUriInvalid" in s or "ServerUri" in s: return False
                await asyncio.sleep(1.0); return False
            finally: self._client = None

        if server_uri_mode == "empty": await try_connect(""); return
        elif server_uri_mode == "raw": await try_connect(override_raw); return
        elif server_uri_mode == "encoded": await try_connect(encode_if_needed(override_raw)); return
        else:
            cand = chosen
            if cand and await try_connect(cand): return
            enc = encode_if_needed(cand)
            if enc and (enc != cand) and await try_connect(enc): return
            await try_connect("")
        self._connected = False

    async def _create_items(self, client, sub, node_ids, sampling, qsize, filter_mode, trigger_s, db_abs, db_pct) -> bool:
        created_any = False
        if filter_mode == "server":
            trig = {"Status": ua.DataChangeTrigger.Status,"Value": ua.DataChangeTrigger.Value,
                    "StatusValue": ua.DataChangeTrigger.StatusValue,"StatusValueTimestamp": ua.DataChangeTrigger.StatusValueTimestamp}.get(trigger_s, ua.DataChangeTrigger.StatusValue)
            requests = []; handle = 1
            for nid in node_ids:
                try:
                    read_id = ua.ReadValueId(); read_id.NodeId = ua.NodeId.from_string(nid); read_id.AttributeId = ua.AttributeIds.Value
                    mparams = ua.MonitoringParameters(); mparams.ClientHandle = handle; mparams.SamplingInterval = float(sampling); mparams.QueueSize = max(1, int(qsize)); mparams.DiscardOldest = True
                    dc_filter = ua.DataChangeFilter(); dc_filter.Trigger = trig
                    if db_abs is not None: dc_filter.DeadbandType = ua.DeadbandType.Absolute; dc_filter.DeadbandValue = float(db_abs)
                    elif db_pct is not None: dc_filter.DeadbandType = ua.DeadbandType.Percent; dc_filter.DeadbandValue = float(db_pct)
                    else: dc_filter.DeadbandType = ua.DeadbandType.None_; dc_filter.DeadbandValue = 0.0
                    mparams.Filter = dc_filter
                    req = ua.MonitoredItemCreateRequest(); req.ItemToMonitor = read_id; req.MonitoringMode = ua.MonitoringMode.Reporting; req.RequestedParameters = mparams
                    requests.append((nid, req)); handle += 1
                except Exception: pass
            if requests:
                try:
                    results = await sub.create_monitored_items(ua.TimestampsToReturn.Both, [r for _, r in requests])
                    for (nid, _req), res in zip(requests, results):
                        code = getattr(res.StatusCode, "value", getattr(res.StatusCode, "name", str(res.StatusCode)))
                        good = getattr(res.StatusCode, "is_good", lambda: False)()
                        self._diag_last_item_result[nid] = str(code)
                        if good: self._diag_created_items += 1; created_any = True
                        else:
                            try: node = client.get_node(nid); await sub.subscribe_data_change(node); self._diag_fallback_items += 1; created_any = True
                            except Exception: pass
                except Exception:
                    for nid, _ in requests:
                        try: node = client.get_node(nid)
                        except Exception: continue
                        try:
                            try: await sub.subscribe_data_change(node, timestamps=ua.TimestampsToReturn.Both)
                            except TypeError: await sub.subscribe_data_change(node)
                            self._diag_fallback_items += 1; created_any = True
                        except Exception: pass
        else:
            for nid in node_ids:
                try:
                    node = client.get_node(nid)
                    try: await sub.subscribe_data_change(node, timestamps=ua.TimestampsToReturn.Both)
                    except TypeError: await sub.subscribe_data_change(node)
                    self._diag_fallback_items += 1; created_any = True
                except Exception: pass
        return created_any

    async def _emit_initial_baseline(self, client, node_ids):
        try:
            for nid in node_ids:
                try:
                    dv = await self._read_value_with_both(client, nid)
                    if dv is None: continue
                    val = getattr(getattr(dv,"Value",None),"Value",getattr(dv,"Value",None))
                    src_ts = getattr(dv,"SourceTimestamp",None); srv_ts = getattr(dv,"ServerTimestamp",None)
                    sc_obj = getattr(dv,"StatusCode",None); status_name = str(getattr(sc_obj,"name","")) or "Unknown"
                    status_code = int(getattr(sc_obj,"value",0)) if sc_obj is not None else 0
                    evt = {"nodeId":nid,"value":val,"dataType":self._datatype_name_from_dv(dv,val),
                           "status":status_code,"statusName":status_name,"statusCode":status_code,
                           "sourceTimestamp":src_ts.isoformat() if src_ts else None,
                           "serverTimestamp":srv_ts.isoformat() if srv_ts else None,"initial":True}
                    self._append_event(evt); self._seeded_nodes.add(nid); self._last_sent[nid]=val
                except Exception: pass
        except Exception: pass

    async def _seed_on_demand(self, node_ids: List[str]):
        client = self._client
        if client is None: return
        for nid in node_ids:
            if nid in self._seeded_nodes: continue
            try:
                dv = await self._read_value_with_both(client, nid)
                if dv is None: continue
                val = getattr(getattr(dv,"Value",None),"Value",getattr(dv,"Value",None))
                src_ts = getattr(dv,"SourceTimestamp",None); srv_ts = getattr(dv,"ServerTimestamp",None)
                sc_obj = getattr(dv,"StatusCode",None); status_name = str(getattr(sc_obj,"name","")) or "Unknown"
                status_code = int(getattr(sc_obj,"value",0)) if sc_obj is not None else 0
                evt = {"nodeId":nid,"value":val,"dataType":self._datatype_name_from_dv(dv,val),
                       "status":status_code,"statusName":status_name,"statusCode":status_code,
                       "sourceTimestamp":src_ts.isoformat() if src_ts else None,
                       "serverTimestamp":srv_ts.isoformat() if srv_ts else None,"initial":True}
                self._append_event(evt); self._last_sent[nid]=val; self._seeded_nodes.add(nid)
            except Exception: pass

    def _write_to_snowflake_direct(self, *, events: List[Dict[str, Any]], endpoint: str,
        account: str, user: str, password: str, role: str,
        warehouse: str, database: str, schema: str, table: str,
        autocreate: bool, authenticator: str,
        private_key_pem: str = "", private_key_pass: str = "") -> int:

        if not events: return 0
        conn_kwargs: Dict[str, Any] = {"account":account,"user":user,"warehouse":warehouse,"database":database,"schema":schema,
                                       "session_parameters":{"TIMESTAMP_TYPE_MAPPING":"TIMESTAMP_TZ"}}
        if role: conn_kwargs["role"]=role
        if authenticator=="snowflake_jwt":
            if serialization is None: raise RuntimeError("cryptography is required for snowflake_jwt but not available.")
            if not private_key_pem.strip(): raise RuntimeError("SF Private Key (PEM) required for snowflake_jwt.")
            pk = serialization.load_pem_private_key(private_key_pem.encode("utf-8"),
                                                    password=(private_key_pass.encode("utf-8") if private_key_pass else None),
                                                    backend=default_backend())
            conn_kwargs.update({"authenticator":"snowflake_jwt","private_key":pk})
        else:
            conn_kwargs.update({"authenticator":"snowflake","password":password})

        iso_now = self._iso_utc()
        params = []
        for e in events:
            params.append((
                e.get("nodeId"),
                json.dumps(e.get("value", None), separators=(",", ":"), ensure_ascii=False),  # VALUE_JSON
                str(e.get("dataType")),            # DATA_TYPE
                str(e.get("statusCode")),              # STATUS (string or number as string)
                str(e.get("statusName")),          # STATUS_NAME
                e.get("sourceTimestamp") or None,  # SOURCE_TIMESTAMP (ISO8601 or None)
                e.get("serverTimestamp") or None,  # SERVER_TIMESTAMP (ISO8601 or None)
            ))

        placeholders = ",".join(["(%s,%s,%s,%s,%s,%s,%s)"] * len(params))
        sql = f"""
        INSERT INTO {database}.{schema}.{table}
        (NODE_ID, VALUE, DATA_TYPE, STATUS, STATUS_NAME, SOURCE_TIMESTAMP, SERVER_TIMESTAMP)
        SELECT
        t.NODE_ID,
        PARSE_JSON(t.VALUE_JSON),
        t.DATA_TYPE,
        t.STATUS,
        t.STATUS_NAME,
        TO_TIMESTAMP_TZ(t.SOURCE_TIMESTAMP),
        TO_TIMESTAMP_TZ(t.SERVER_TIMESTAMP)
        FROM VALUES
        {placeholders}
        AS t(NODE_ID, VALUE_JSON, DATA_TYPE, STATUS, STATUS_NAME, SOURCE_TIMESTAMP, SERVER_TIMESTAMP)
        """
        with sf_connect(**conn_kwargs) as con:
            if autocreate: self._ensure_sf_table(con, database, schema, table)
            cur = con.cursor()
            try:
                cur.execute(sql, tuple(x for tup in params for x in tup))
                return int(len(params))
            finally:
                cur.close()

    def _ensure_sf_table(self, con, db: str, schema: str, table: str):
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {db}.{schema}.{table} (
            NODE_ID STRING, VALUE VARIANT, STATUS STRING, SOURCE_TS TIMESTAMP_TZ, ENDPOINT STRING, INGEST_TS TIMESTAMP_TZ
        )"""
        try:
            cur = con.cursor()
            try: cur.execute(ddl)
            finally: cur.close()
        except Exception: pass

    def _append_event(self, evt: Dict[str, Any]):
        with self._buf_lock:
            before = len(self._buffer); self._buffer.append(evt)
            if len(self._buffer)==self._buffer_max and before==self._buffer_max: self._dropped_events += 1
        self._notify_event.set()

    def _get(self, context, name: str, default: Optional[str] = None) -> Optional[str]:
        try:
            pv = context.getProperty(name)
            if pv is None: return default
            if hasattr(pv,"evaluateAttributeExpressions"): pv = pv.evaluateAttributeExpressions()
            val = pv.getValue() if hasattr(pv,"getValue") else pv
            return default if val in (None,"") else val
        except Exception: return default

    def _iso_utc(self) -> str: return datetime.now(timezone.utc).isoformat()

    def _to_float_or_none(self, s: Optional[str]) -> Optional[float]:
        if s is None: return None
        s2 = str(s).strip()
        if s2 == "": return None
        try: return float(s2)
        except Exception: return None

    def _csv_cell(self, v: Any, delim: str) -> str:
        s = "" if v is None else str(v)
        needs_quote = any(ch in s for ch in (delim, '"', "\n", "\r"))
        s2 = s.replace('"','""')
        return f'"{s2}"' if needs_quote else s2

    def _route_failure(self, msg: str) -> FlowFileSourceResult:
        return FlowFileSourceResult("failure", {"mime.type": "text/plain"}, str(msg).encode("utf-8","ignore"))

    def _sanitize_uri(self, value: str) -> str:
        if not value or not isinstance(value, str): return ""
        return quote(value.strip(), safe=":/@!$&'()*+,;=-._~")

    def _apply_server_uri_soft(self, client, value: str) -> None:
        uri = (value or "").strip()
        try:
            if hasattr(client,"set_server_uri"): client.set_server_uri(uri)
        except Exception: pass
        for target in (client, getattr(client,"uaclient",None)):
            try:
                if target is not None: setattr(target,"server_uri",uri)
            except Exception: pass

    def _patch_create_session_hard(self, client, forced_uri: str) -> None:
        try:
            orig = getattr(client.uaclient,"create_session",None)
            if orig is None: return
            async def _create_session_patched(params,*args,**kwargs):
                try: params.ServerUri = forced_uri
                except Exception: pass
                return await orig(params,*args,**kwargs)
            client.uaclient.create_session = _create_session_patched  # type: ignore
        except Exception: pass

    async def _read_value_with_both(self, client, nid: str):
        try:
            rvid = ua.ReadValueId(); rvid.NodeId = ua.NodeId.from_string(nid); rvid.AttributeId = ua.AttributeIds.Value
            uac = getattr(client,"uaclient",client)
            try:
                dvs = await uac.read([rvid], max_age=0, timestamps=ua.TimestampsToReturn.Both)
                if dvs and dvs[0] is not None: return dvs[0]
            except TypeError:
                dvs = await uac.read([rvid], max_age=0)
                if dvs and dvs[0] is not None: return dvs[0]
            except Exception: pass
            try:
                node = client.get_node(nid)
                try: return await node.read_data_value(timestamps=ua.TimestampsToReturn.Both)
                except TypeError: return await node.read_data_value()
            except Exception: return None
        except Exception: return None

    def _datatype_name_from_dv(self, dv, value_obj) -> str:
        try:
            v = getattr(dv,"Value",None); vt = getattr(v,"VariantType",None); name = getattr(vt,"name",None)
            if name: return str(name)
        except Exception: pass
        val = value_obj
        try:
            import numpy as _np
            if isinstance(val, (_np.floating,)): return "Double"
            if isinstance(val, (_np.integer,)):  return "Int64"
        except Exception: pass
        if isinstance(val,bool): return "Boolean"
        if isinstance(val,float): return "Double"
        if isinstance(val,int): return "Int64"
        if isinstance(val,str): return "String"
        if val is None: return "Null"
        return type(val).__name__

    def _normalize_account(self, s: str) -> str:
        s = (s or "").strip().replace("https://","").replace("http://","")
        if ".snowflakecomputing.com" in s: s = s.split(".snowflakecomputing.com")[0]
        s = s.split(":")[0].split("/")[0]
        return s

class _Callback:
    def __init__(self, proc: OPCUASubscribe, filter_mode: str, db_abs: Optional[float], db_pct: Optional[float]):
        self.proc = proc; self.filter_mode = (filter_mode or "client").lower(); self.db_abs = db_abs; self.db_pct = db_pct
    def datachange_notification(self, node, val, data):
        try:
            try: nid = getattr(getattr(node,"nodeid",None),"to_string",lambda: str(node))()
            except Exception: nid = str(node)
            dv = getattr(getattr(data,"monitored_item",None),"Value",None)
            if dv is None and hasattr(data,"Value"): dv = data
            if dv is None: dv = getattr(data,"value",None)
            src_ts = getattr(dv,"SourceTimestamp",None) if dv is not None else None
            srv_ts = getattr(dv,"ServerTimestamp",None) if dv is not None else None
            if src_ts is None: src_ts = getattr(data,"SourceTimestamp",None) or getattr(data,"source_ts",None)
            if srv_ts is None: srv_ts = getattr(data,"ServerTimestamp",None) or getattr(data,"server_ts",None)
            sc_obj = getattr(dv,"StatusCode",None) if dv is not None else None
            if sc_obj is None: sc_obj = getattr(data,"StatusCode",None) or getattr(data,"status",None)
            status_name = str(getattr(sc_obj,"name","")) or "Unknown"
            status_code = int(getattr(sc_obj,"value",0)) if sc_obj is not None else 0

            if self.filter_mode == "client" and (self.db_abs is not None or self.db_pct is not None):
                last = self.proc._last_sent.get(nid, None)
                if last is not None:
                    try:
                        f_new = float(val); f_old = float(last); delta = abs(f_new - f_old)
                        abs_ok = True if self.db_abs is None else delta >= float(self.db_abs)
                        pct_ok = True
                        if self.db_pct is not None:
                            base = abs(f_old); pct_ok = (delta / base * 100.0) >= float(self.db_pct) if base != 0 else True
                        if not (abs_ok and pct_ok): return
                    except Exception:
                        if val == last: return

            evt = {
                "nodeId": nid, "value": val,
                "dataType": self.proc._datatype_name_from_dv(dv if dv is not None else data, val),
                "status": status_name, "statusName": status_name, "statusCode": status_code,
                "sourceTimestamp": src_ts.isoformat() if hasattr(src_ts,"isoformat") else None,
                "serverTimestamp": srv_ts.isoformat() if hasattr(srv_ts,"isoformat") else None,
            }
            self.proc._append_event(evt); self.proc._last_sent[nid] = val
        except Exception: pass

def create(**kwargs): return OPCUASubscribe(**kwargs)