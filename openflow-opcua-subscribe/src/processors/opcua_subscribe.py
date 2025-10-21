# opcua_subscribe.py — now supports JSON / CSV / XML / Snowflake sinks
from __future__ import annotations

import asyncio
import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from collections import deque
from threading import Lock, Thread, Event
from urllib.parse import urlparse, quote
import io

# ---- NiFi / OpenFlow API ----
from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.relationship import Relationship

# ---- Optional deps (guarded) ----
try:
    from asyncua import Client, ua  # type: ignore
except Exception:  # pragma: no cover
    Client = None
    ua = None

try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None

try:
    import pyarrow as pa  # noqa: F401
except Exception:
    pa = None

try:
    from snowflake.connector import connect as sf_connect  # type: ignore
    from snowflake.connector.pandas_tools import write_pandas  # type: ignore
except Exception:
    sf_connect = None
    write_pandas = None


__all__ = ["OPCUASubscribe", "create"]


class OPCUASubscribe(FlowFileSource):
    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileSource"]

    class ProcessorDetails:
        version = "1.0.0"
        description = (
            "Continuous OPC UA subscription with client/server deadband options. "
            "Output as JSON/CSV/XML or write directly to Snowflake."
        )
        tags = ["opcua", "subscribe", "deadband", "source", "openflow", "nifi", "snowflake"]
        # Keep minimal hard deps for discoverability; others are guarded above
        dependencies = ["asyncua>=1.0.0,<2.0.0", "cryptography>=41,<43"]

    # --------------- Relationships ---------------
    REL_SUCCESS = Relationship("success", "Payload with changes, or Snowflake write summary.")
    REL_EMPTY   = Relationship("empty",   "No changes in the wait window (heartbeat JSON or 0-byte).")
    REL_FAILURE = Relationship("failure", "Text error payload.")

    def get_relationships(self):
        return [self.REL_SUCCESS, self.REL_EMPTY, self.REL_FAILURE]

    def getRelationships(self):
        return self.get_relationships()

    # --------------- Init (no I/O) ---------------
    def __init__(self, **kwargs):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)

        # Background subscriber
        self._bg_thread: Optional[Thread] = None
        self._bg_loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop_flag: bool = False
        self._connected: bool = False

        # Event buffer + signaling
        self._buffer_max: int = 50000
        self._buffer: deque = deque(maxlen=self._buffer_max)
        self._buf_lock = Lock()
        self._notify_event = Event()
        self._dropped_events: int = 0

        # Last emitted value (client-side deadband baseline)
        self._last_sent: Dict[str, Any] = {}

        # Restart signature
        self._active_signature: Optional[str] = None

        # Diagnostics
        self._diag_last_start_err: Optional[str] = None
        self._diag_last_item_result: Dict[str, str] = {}
        self._diag_created_items: int = 0
        self._diag_fallback_items: int = 0

        # Effective URI used
        self._last_effective_server_uri: Optional[str] = None

    # --------------- Properties ---------------
    def get_property_descriptors(self):
        try:
            V = StandardValidators
            non_empty = getattr(V, "NON_EMPTY_VALIDATOR", None)
            pos_int = getattr(V, "POSITIVE_INTEGER_VALIDATOR", None)
            bool_val = getattr(V, "BOOLEAN_VALIDATOR", None)

            # Connection & session
            self.P_ENDPOINT = PropertyDescriptor(
                name="OPC UA Endpoint", description="opc.tcp://host:port/Path",
                required=True, validators=[non_empty] if non_empty else []
            )
            self.P_SERVER_URI = PropertyDescriptor(
                name="Server URI Override",
                description=("Exact ApplicationUri to send in CreateSession.ServerUri. "
                             "If blank, auto-discover from GetEndpoints."),
                required=False, default_value="",
            )
            self.P_SERVER_URI_MODE = PropertyDescriptor(
                name="Server URI Mode",
                description=("How to send ServerUri in CreateSession: "
                             "auto, raw, encoded, empty."),
                required=False, default_value="auto",
                allowable_values=["auto", "raw", "encoded", "empty"],
            )

            # Subscribe
            self.P_NODEIDS = PropertyDescriptor(
                name="NodeIds (CSV)",
                description='Comma-separated NodeIds (e.g., "ns=2;s=Tag1,ns=2;s=Tag2").',
                required=True, validators=[non_empty] if non_empty else []
            )
            self.P_SAMPLING = PropertyDescriptor(
                name="Sampling Interval (ms)",
                description="Requested sampling interval for monitored items.",
                required=False, default_value="250", validators=[pos_int] if pos_int else []
            )
            self.P_QUEUE = PropertyDescriptor(
                name="Monitored Item Queue Size",
                description="Server queue size per item (>=1).",
                required=False, default_value="256", validators=[pos_int] if pos_int else []
            )
            self.P_FILTER_MODE = PropertyDescriptor(
                name="Filter Mode",
                description="Where to apply deadband/trigger filtering.",
                required=False, default_value="client",
                allowable_values=["client", "server"]
            )
            self.P_TRIGGER = PropertyDescriptor(
                name="Trigger",
                description="Server-side DataChange trigger (server mode only).",
                required=False, default_value="StatusValue",
                allowable_values=["Status", "Value", "StatusValue", "StatusValueTimestamp"]
            )
            self.P_DEADBAND_ABS = PropertyDescriptor(
                name="Deadband Absolute",
                description="Absolute threshold (client-side in 'client' mode, server-side in 'server' mode).",
                required=False, default_value=""
            )
            self.P_DEADBAND_PCT = PropertyDescriptor(
                name="Deadband Percent",
                description="Percent threshold (0-100). Often requires node EURange for server-side.",
                required=False, default_value=""
            )
            self.P_BLOCK_MS = PropertyDescriptor(
                name="Block Until Event (ms)",
                description="How long onTrigger waits for a pushed event before returning.",
                required=False, default_value="5000", validators=[pos_int] if pos_int else []
            )
            self.P_EMIT_INITIAL = PropertyDescriptor(
                name="Emit Initial On Connect",
                description="After (re)connect, emit one initial sample per NodeId to seed the baseline.",
                required=False, default_value="true", validators=[bool_val] if bool_val else []
            )
            self.P_MAX_BUF = PropertyDescriptor(
                name="Max Client Buffer Events",
                description="Max events retained client-side between trigger ticks.",
                required=False, default_value="50000", validators=[pos_int] if pos_int else []
            )
            self.P_TIMEOUT = PropertyDescriptor(
                name="Timeout (sec)", description="Client operation timeout.",
                required=False, default_value="10", validators=[pos_int] if pos_int else []
            )
            self.P_USERNAME = PropertyDescriptor(name="Username", description="Optional", required=False, default_value="")
            self.P_PASSWORD = PropertyDescriptor(name="Password", description="Optional", required=False, default_value="", sensitive=True)

            # -------- Output selection --------
            self.P_OUTPUT_FMT = PropertyDescriptor(
                name="Output Format",
                description="Where/how to emit results.",
                required=False, default_value="json",
                allowable_values=["json", "csv", "xml", "snowflake"],
            )

            # CSV
            self.P_CSV_DELIM = PropertyDescriptor(
                name="CSV Delimiter",
                description="Single-character delimiter for CSV output.",
                required=False, default_value=","
            )
            self.P_CSV_HEADER = PropertyDescriptor(
                name="CSV Include Header",
                description="Emit a header line for CSV output.",
                required=False, default_value="true", validators=[bool_val] if bool_val else []
            )

            # XML
            self.P_XML_ROOT = PropertyDescriptor(
                name="XML Root Element",
                description="Root element name for XML output.",
                required=False, default_value="opcuaEvents"
            )
            self.P_XML_ROW = PropertyDescriptor(
                name="XML Row Element",
                description="Row element name for XML output.",
                required=False, default_value="event"
            )

            # Snowflake
            self.P_SF_ACCOUNT = PropertyDescriptor(name="SF Account", description="Snowflake account identifier", required=False, default_value="")
            self.P_SF_USER    = PropertyDescriptor(name="SF User", description="Snowflake user", required=False, default_value="")
            self.P_SF_PASS    = PropertyDescriptor(name="SF Password", description="Snowflake password", required=False, default_value="", sensitive=True)
            self.P_SF_ROLE    = PropertyDescriptor(name="SF Role", description="Optional role", required=False, default_value="")
            self.P_SF_WH      = PropertyDescriptor(name="SF Warehouse", description="Warehouse", required=False, default_value="")
            self.P_SF_DB      = PropertyDescriptor(name="SF Database", description="Database", required=False, default_value="")
            self.P_SF_SCHEMA  = PropertyDescriptor(name="SF Schema", description="Schema", required=False, default_value="")
            self.P_SF_TABLE   = PropertyDescriptor(name="SF Table", description="Target table for events", required=False, default_value="")
            self.P_SF_AUTOCREATE = PropertyDescriptor(
                name="SF Auto-Create Table",
                description="Create table if it does not exist (NodeId STRING, Value VARIANT, Status STRING, SourceTs TIMESTAMP_TZ, Endpoint STRING, IngestTs TIMESTAMP_TZ).",
                required=False, default_value="true", validators=[bool_val] if bool_val else []
            )

            self.P_HEARTBEAT = PropertyDescriptor(
                name="Emit Heartbeat on Empty",
                description="When no events, route to 'empty' with a minimal heartbeat JSON (true) or 0-byte (false).",
                required=False, default_value="true", validators=[bool_val] if bool_val else []
            )
            self.P_DEBUG = PropertyDescriptor(
                name="Debug Level",
                description="none | basic | verbose. Controls diagnostic attributes.",
                required=False, default_value="basic", allowable_values=["none", "basic", "verbose"]
            )

            return [
                # Core
                self.P_ENDPOINT, self.P_SERVER_URI, self.P_SERVER_URI_MODE,
                self.P_NODEIDS, self.P_SAMPLING, self.P_QUEUE,
                self.P_FILTER_MODE, self.P_TRIGGER, self.P_DEADBAND_ABS, self.P_DEADBAND_PCT,
                self.P_BLOCK_MS, self.P_EMIT_INITIAL, self.P_MAX_BUF,
                self.P_TIMEOUT, self.P_USERNAME, self.P_PASSWORD,
                # Output
                self.P_OUTPUT_FMT, self.P_CSV_DELIM, self.P_CSV_HEADER, self.P_XML_ROOT, self.P_XML_ROW,
                # Snowflake
                self.P_SF_ACCOUNT, self.P_SF_USER, self.P_SF_PASS, self.P_SF_ROLE,
                self.P_SF_WH, self.P_SF_DB, self.P_SF_SCHEMA, self.P_SF_TABLE, self.P_SF_AUTOCREATE,
                # Etc
                self.P_HEARTBEAT, self.P_DEBUG
            ]
        except Exception:
            # Minimal fallback so the processor still shows in UI
            return [PropertyDescriptor(name="OPC UA Endpoint", description="Fallback", required=True)]

    def getPropertyDescriptors(self):
        return self.get_property_descriptors()

    # --------------- Trigger ---------------
    def create(self, context):  # FlowFileSource compatibility
        return self.on_trigger(context)

    def onTrigger(self, context):
        return self.on_trigger(context)

    def on_trigger(self, context):
        try:
            if Client is None or ua is None:
                return self._route_failure("Missing asyncua dependency. Vendor it into the NAR.")

            # ---- read common props ----
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
            heartbeat = (self._get(context, "Emit Heartbeat on Empty", "true") or "true").lower() == "true"
            debug     = (self._get(context, "Debug Level", "basic") or "basic").lower()

            server_uri_override = (self._get(context, "Server URI Override", "") or "").strip()
            server_uri_mode = (self._get(context, "Server URI Mode", "auto") or "auto").strip().lower()

            out_fmt   = (self._get(context, "Output Format", "json") or "json").lower()
            csv_delim = (self._get(context, "CSV Delimiter", ",") or ",")
            csv_header = (self._get(context, "CSV Include Header", "true") or "true").lower() == "true"
            xml_root  = (self._get(context, "XML Root Element", "opcuaEvents") or "opcuaEvents")
            xml_row   = (self._get(context, "XML Row Element", "event") or "event")

            # Snowflake props
            sf_account = self._get(context, "SF Account", "") or ""
            sf_user    = self._get(context, "SF User", "") or ""
            sf_pass    = self._get(context, "SF Password", "") or ""
            sf_role    = self._get(context, "SF Role", "") or ""
            sf_wh      = self._get(context, "SF Warehouse", "") or ""
            sf_db      = self._get(context, "SF Database", "") or ""
            sf_schema  = self._get(context, "SF Schema", "") or ""
            sf_table   = self._get(context, "SF Table", "") or ""
            sf_autocreate = (self._get(context, "SF Auto-Create Table", "true") or "true").lower() == "true"

            if not endpoint or not node_ids:
                return self._route_failure("OPC UA Endpoint and NodeIds (CSV) are required.")

            # live resize client buffer
            if max_buf != self._buffer_max and max_buf > 0:
                with self._buf_lock:
                    tail = list(self._buffer)[-max_buf:]
                    self._buffer = deque(tail, maxlen=max_buf)
                    self._buffer_max = max_buf

            # (Re)start background if signature changed or disconnected
            signature = json.dumps({
                "endpoint": endpoint, "node_ids": node_ids, "sampling": sampling, "qsize": qsize,
                "filter_mode": filter_mode, "trigger": trigger_s, "db_abs": db_abs, "db_pct": db_pct,
                "timeout": timeout_s, "username": bool(username), "emit_init": emit_init,
                "server_uri_override": server_uri_override, "server_uri_mode": server_uri_mode
            }, sort_keys=True)

            if signature != self._active_signature or not self._connected:
                self._restart_background(
                    endpoint=endpoint, node_ids=node_ids, sampling=sampling, qsize=qsize,
                    filter_mode=filter_mode, trigger_s=trigger_s, db_abs=db_abs, db_pct=db_pct,
                    timeout_s=timeout_s, username=username, password=password, emit_init=emit_init,
                    server_uri_override=server_uri_override, server_uri_mode=server_uri_mode
                )

            # Drain events
            out_events: List[Dict[str, Any]] = []
            with self._buf_lock:
                while self._buffer:
                    out_events.append(self._buffer.popleft())

            # Block for event if empty
            if not out_events:
                self._notify_event.clear()
                self._notify_event.wait(timeout=max(0.0, block_ms / 1000.0))
                with self._buf_lock:
                    while self._buffer:
                        out_events.append(self._buffer.popleft())

            # Build common attributes
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

            if self._diag_last_start_err and debug in ("basic", "verbose"):
                attrs["opcua.last_start_error"] = self._diag_last_start_err
            if self._diag_last_item_result and debug == "verbose":
                attrs["opcua.item_results.all"] = json.dumps(self._diag_last_item_result, separators=(",", ":"))

            # Route empty
            if not out_events:
                attrs["opcua.empty"] = "true"
                if heartbeat:
                    payload = {
                        "mode": "subscribe", "endpoint": endpoint, "timestamp_utc": self._iso_utc(),
                        "events": [], "heartbeat": True
                    }
                    data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
                    attrs["mime.type"] = "application/json"
                    return FlowFileSourceResult("empty", attrs, data)
                return FlowFileSourceResult("empty", attrs, b"")

            # ---------- OUTPUT SWITCH ----------
            attrs["opcua.empty"] = "false"
            if out_fmt == "json":
                payload = {
                    "mode": "subscribe", "endpoint": endpoint, "timestamp_utc": self._iso_utc(),
                    "events": out_events
                }
                data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
                attrs["mime.type"] = "application/json"
                return FlowFileSourceResult("success", attrs, data)

            elif out_fmt == "csv":
                # flat rows
                cols = ["nodeId", "value", "status", "sourceTimestamp"]
                buf = io.StringIO()
                if csv_header:
                    buf.write(csv_delim.join(cols) + "\n")
                for e in out_events:
                    row = [
                        self._csv_cell(e.get("nodeId"), csv_delim),
                        self._csv_cell(e.get("value"), csv_delim),
                        self._csv_cell(e.get("status"), csv_delim),
                        self._csv_cell(e.get("sourceTimestamp"), csv_delim),
                    ]
                    buf.write(csv_delim.join(row) + "\n")
                data = buf.getvalue().encode("utf-8")
                attrs["mime.type"] = "text/csv"
                attrs["csv.delimiter"] = csv_delim
                attrs["csv.header"] = str(csv_header).lower()
                return FlowFileSourceResult("success", attrs, data)

            elif out_fmt == "xml":
                # very simple XML; values are text-escaped
                def esc(v):
                    s = "" if v is None else str(v)
                    return (s.replace("&", "&amp;")
                             .replace("<", "&lt;")
                             .replace(">", "&gt;")
                             .replace('"', "&quot;")
                             .replace("'", "&apos;"))
                ts = self._iso_utc()
                parts = [f'<?xml version="1.0" encoding="UTF-8"?>',
                         f'<{xml_root} endpoint="{esc(endpoint)}" timestamp_utc="{esc(ts)}">']
                for e in out_events:
                    parts.append(f'  <{xml_row}>')
                    parts.append(f'    <nodeId>{esc(e.get("nodeId"))}</nodeId>')
                    parts.append(f'    <value>{esc(e.get("value"))}</value>')
                    parts.append(f'    <status>{esc(e.get("status"))}</status>')
                    parts.append(f'    <sourceTimestamp>{esc(e.get("sourceTimestamp"))}</sourceTimestamp>')
                    parts.append(f'  </{xml_row}>')
                parts.append(f'</{xml_root}>')
                data = ("\n".join(parts)).encode("utf-8")
                attrs["mime.type"] = "application/xml"
                return FlowFileSourceResult("success", attrs, data)

            elif out_fmt == "snowflake":
                # write to Snowflake; return a small JSON summary
                if sf_connect is None or write_pandas is None or pd is None:
                    return self._route_failure(
                        "Snowflake sink selected but snowflake-connector-python and pandas are not bundled."
                    )
                if not (sf_account and sf_user and sf_pass and sf_wh and sf_db and sf_schema and sf_table):
                    return self._route_failure(
                        "Snowflake sink selected but one or more required connection properties are missing "
                        "(account, user, password, warehouse, database, schema, table)."
                    )

                rows_written = self._write_to_snowflake(
                    events=out_events, endpoint=endpoint,
                    account=sf_account, user=sf_user, password=sf_pass, role=sf_role,
                    warehouse=sf_wh, database=sf_db, schema=sf_schema, table=sf_table,
                    autocreate=sf_autocreate
                )
                summary = {
                    "mode": "subscribe", "endpoint": endpoint, "timestamp_utc": self._iso_utc(),
                    "events_written": rows_written, "table": f"{sf_db}.{sf_schema}.{sf_table}"
                }
                attrs["mime.type"] = "application/json"
                attrs["snowflake.rows_written"] = str(rows_written)
                attrs["snowflake.table"] = f"{sf_db}.{sf_schema}.{sf_table}"
                return FlowFileSourceResult("success", attrs, json.dumps(summary).encode("utf-8"))

            else:
                return self._route_failure(f"Unsupported Output Format: {out_fmt}")

        except Exception as e:
            return self._route_failure(f"{type(e).__name__}: {e}\n{traceback.format_exc()}")

    # --------------- Lifecycle ---------------
    def onStopped(self): self._stop_background(True)
    def onStop(self):    self._stop_background(True)
    def onUnscheduled(self): self._stop_background(True)
    def stop(self):      self._stop_background(True)

    # --------------- Background subscriber ---------------
    def _restart_background(self, *, endpoint: str, node_ids: List[str], sampling: int, qsize: int,
                            filter_mode: str, trigger_s: str, db_abs: Optional[float], db_pct: Optional[float],
                            timeout_s: int, username: Optional[str], password: Optional[str], emit_init: bool,
                            server_uri_override: str, server_uri_mode: str):
        self._stop_background(True)
        self._stop_flag = False
        self._connected = False
        self._active_signature = json.dumps({
            "endpoint": endpoint, "node_ids": node_ids, "sampling": sampling, "qsize": qsize,
            "filter_mode": filter_mode, "trigger": trigger_s, "db_abs": db_abs, "db_pct": db_pct,
            "timeout": timeout_s, "username": bool(username), "emit_init": emit_init,
            "server_uri_override": server_uri_override, "server_uri_mode": server_uri_mode
        }, sort_keys=True)

        def runner():
            try:
                loop = asyncio.new_event_loop()
                self._bg_loop = loop
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
                    if self._bg_loop and not self._bg_loop.is_closed():
                        self._bg_loop.close()
                except Exception:
                    pass
                self._bg_loop = None

        self._bg_thread = Thread(target=runner, name=f"{self.__class__.__name__}-subscriber", daemon=True)
        self._bg_thread.start()

    def _stop_background(self, join: bool):
        try:
            self._stop_flag = True
            if self._bg_loop:
                try:
                    self._bg_loop.call_soon_threadsafe(lambda: None)
                except Exception:
                    pass
        except Exception:
            pass
        if join and self._bg_thread and self._bg_thread.is_alive():
            try:
                self._bg_thread.join(timeout=2.0)
            except Exception:
                pass
        self._bg_thread = None
        self._connected = False

    async def _subscriber_loop(self, *, endpoint: str, node_ids: List[str], sampling: int, qsize: int,
                               filter_mode: str, trigger_s: str, db_abs: Optional[float], db_pct: Optional[float],
                               timeout_s: int, username: Optional[str], password: Optional[str], emit_init: bool,
                               server_uri_override: str, server_uri_mode: str):
        if Client is None or ua is None:
            return

        # --- URI selection & discovery (aligned with your browser processor) ---
        override_raw = (server_uri_override or "").strip()

        async def discover_app_uri() -> str:
            try:
                target = self._normalize_endpoint(endpoint)
                eps = await Client(url=endpoint, timeout=timeout_s).connect_and_get_server_endpoints()
                best = ""
                for ep in eps:
                    ep_url = str(getattr(ep, "EndpointUrl", getattr(ep, "endpoint_url", "")))
                    ep_norm = self._normalize_endpoint(ep_url)
                    server_desc = getattr(ep, "Server", getattr(ep, "server", None))
                    app_uri = None
                    if server_desc is not None:
                        app_uri = getattr(server_desc, "ApplicationUri", getattr(server_desc, "application_uri", None))
                    if ep_norm == target and app_uri:
                        return str(app_uri)
                    if not best and app_uri:
                        best = str(app_uri)
                return best
            except Exception:
                return ""

        def encode_if_needed(v: str) -> str:
            return self._sanitize_uri(v)

        async def pick_auto() -> str:
            return (override_raw or (await discover_app_uri()) or "").strip()

        # compute preferred candidate
        if server_uri_mode == "empty":
            chosen = ""
        elif server_uri_mode == "raw":
            chosen = override_raw
        elif server_uri_mode == "encoded":
            chosen = encode_if_needed(override_raw)
        else:
            chosen = await pick_auto()

        # helper to try connect with a specific ServerUri
        async def try_connect(with_uri: str):
            client = Client(url=endpoint, timeout=timeout_s)
            self._apply_server_uri_soft(client, with_uri)
            self._patch_create_session_hard(client, with_uri)
            self._last_effective_server_uri = with_uri
            if username:
                try:
                    client.set_user(username)
                    if password:
                        client.set_password(password)
                except Exception:
                    pass

            try:
                async with client:
                    self._connected = True
                    self._diag_created_items = 0
                    self._diag_fallback_items = 0
                    self._diag_last_item_result = {}

                    sub = await client.create_subscription(sampling, _Callback(self, filter_mode, db_abs, db_pct))
                    try:
                        created_any = await self._create_items(client, sub, node_ids, sampling, qsize,
                                                               filter_mode, trigger_s, db_abs, db_pct)
                        if not created_any:
                            return False

                        if emit_init:
                            await self._emit_initial_baseline(client, node_ids)

                        while not self._stop_flag:
                            await asyncio.sleep(0.2)
                    finally:
                        try:
                            await sub.delete()
                        except Exception:
                            pass
                return True
            except Exception as e:
                s = str(e)
                self._connected = False
                if "BadServerUriInvalid" in s or "ServerUri" in s:
                    return False
                await asyncio.sleep(1.0)
                return False

        # connection attempts
        if server_uri_mode == "empty":
            await try_connect("")
            return
        elif server_uri_mode == "raw":
            await try_connect(override_raw)
            return
        elif server_uri_mode == "encoded":
            await try_connect(encode_if_needed(override_raw))
            return
        else:
            cand = chosen
            if cand and await try_connect(cand):
                return
            enc = encode_if_needed(cand)
            if enc and (enc != cand) and await try_connect(enc):
                return
            await try_connect("")

        self._connected = False

    async def _create_items(self, client, sub, node_ids, sampling, qsize,
                            filter_mode, trigger_s, db_abs, db_pct) -> bool:
        created_any = False
        if filter_mode == "server":
            trig = {
                "Status": ua.DataChangeTrigger.Status,
                "Value": ua.DataChangeTrigger.Value,
                "StatusValue": ua.DataChangeTrigger.StatusValue,
                "StatusValueTimestamp": ua.DataChangeTrigger.StatusValueTimestamp,
            }.get(trigger_s, ua.DataChangeTrigger.StatusValue)

            requests = []
            handle = 1
            for nid in node_ids:
                try:
                    read_id = ua.ReadValueId()
                    read_id.NodeId = ua.NodeId.from_string(nid)
                    read_id.AttributeId = ua.AttributeIds.Value

                    mparams = ua.MonitoringParameters()
                    mparams.ClientHandle = handle
                    mparams.SamplingInterval = float(sampling)
                    mparams.QueueSize = max(1, int(qsize))
                    mparams.DiscardOldest = True

                    dc_filter = ua.DataChangeFilter()
                    dc_filter.Trigger = trig
                    if db_abs is not None:
                        dc_filter.DeadbandType = ua.DeadbandType.Absolute
                        dc_filter.DeadbandValue = float(db_abs)
                    elif db_pct is not None:
                        dc_filter.DeadbandType = ua.DeadbandType.Percent
                        dc_filter.DeadbandValue = float(db_pct)
                    else:
                        dc_filter.DeadbandType = ua.DeadbandType.None_
                        dc_filter.DeadbandValue = 0.0
                    mparams.Filter = dc_filter

                    req = ua.MonitoredItemCreateRequest()
                    req.ItemToMonitor = read_id
                    req.MonitoringMode = ua.MonitoringMode.Reporting
                    req.RequestedParameters = mparams
                    requests.append((nid, req))
                    handle += 1
                except Exception:
                    pass

            if requests:
                try:
                    results = await sub.create_monitored_items(
                        ua.TimestampsToReturn.Both, [r for _, r in requests]
                    )
                    for (nid, _req), res in zip(requests, results):
                        code = getattr(res.StatusCode, "value", getattr(res.StatusCode, "name", str(res.StatusCode)))
                        good = getattr(res.StatusCode, "is_good", lambda: False)()
                        self._diag_last_item_result[nid] = str(code)
                        if good:
                            self._diag_created_items += 1
                            created_any = True
                        else:
                            # Fallback to simple subscribe (no filter)
                            try:
                                node = client.get_node(nid)
                                await sub.subscribe_data_change(node)
                                self._diag_fallback_items += 1
                                created_any = True
                            except Exception:
                                pass
                except Exception:
                    # Massive fallback to simple subscribe
                    for nid, _ in requests:
                        try:
                            node = client.get_node(nid)
                            await sub.subscribe_data_change(node)
                            self._diag_fallback_items += 1
                            created_any = True
                        except Exception:
                            pass
        else:
            # client mode: always simple subscribe
            for nid in node_ids:
                try:
                    node = client.get_node(nid)
                    await sub.subscribe_data_change(node)
                    self._diag_fallback_items += 1  # counted as simple path
                    created_any = True
                except Exception:
                    pass

        return created_any

    async def _emit_initial_baseline(self, client, node_ids):
        try:
            for nid in node_ids:
                try:
                    node = client.get_node(nid)
                    dv = await node.read_data_value()
                    val = getattr(dv, "Value", None)
                    val = getattr(val, "Value", val)
                    ts = getattr(dv, "SourceTimestamp", None)
                    evt = {
                        "nodeId": nid,
                        "value": val,
                        "status": str(getattr(getattr(dv, "StatusCode", None), "name", "")) or "Unknown",
                        "sourceTimestamp": ts.isoformat() if ts else None,
                        "initial": True,
                    }
                    self._append_event(evt)
                    self._last_sent[nid] = val
                except Exception:
                    pass
        except Exception:
            pass

    # --------------- Snowflake sink ---------------
    def _write_to_snowflake(
        self, *, events: List[Dict[str, Any]], endpoint: str,
        account: str, user: str, password: str, role: str,
        warehouse: str, database: str, schema: str, table: str,
        autocreate: bool
    ) -> int:
        # Prepare dataframe
        if pd is None:
            raise RuntimeError("pandas not available")
        df = pd.DataFrame([
            {
                "NODE_ID": e.get("nodeId"),
                "VALUE": e.get("value"),
                "STATUS": e.get("status"),
                "SOURCE_TS": e.get("sourceTimestamp"),
                "ENDPOINT": endpoint,
                "INGEST_TS": self._iso_utc(),
            } for e in events
        ])

        conn_kwargs = {
            "account": account, "user": user, "password": password,
            "warehouse": warehouse, "database": database, "schema": schema
        }
        if role:
            conn_kwargs["role"] = role

        with sf_connect(**conn_kwargs) as con:
            if autocreate:
                self._ensure_sf_table(con, database, schema, table)
            # write_pandas returns (success, nchunks, nrows, output)
            ok, _chunks, nrows, _out = write_pandas(
                con, df, table_name=table, database=database, schema=schema, auto_create_table=False
            )
            if not ok:
                raise RuntimeError("write_pandas reported failure")
            return int(nrows or 0)

    def _ensure_sf_table(self, con, db: str, schema: str, table: str) -> None:
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {db}.{schema}.{table} (
            NODE_ID STRING,
            VALUE VARIANT,
            STATUS STRING,
            SOURCE_TS TIMESTAMP_TZ,
            ENDPOINT STRING,
            INGEST_TS TIMESTAMP_TZ
        )
        """
        try:
            cur = con.cursor()
            try:
                cur.execute(ddl)
            finally:
                cur.close()
        except Exception:
            # best-effort
            pass

    # --------------- Buffer & Utils ---------------
    def _append_event(self, evt: Dict[str, Any]):
        with self._buf_lock:
            before_len = len(self._buffer)
            self._buffer.append(evt)
            if len(self._buffer) == self._buffer_max and before_len == self._buffer_max:
                self._dropped_events += 1
        self._notify_event.set()

    def _get(self, context, name: str, default: Optional[str] = None) -> Optional[str]:
        try:
            pv = context.getProperty(name)
            if pv is None:
                return default
            if hasattr(pv, "evaluateAttributeExpressions"):
                pv = pv.evaluateAttributeExpressions()
            val = pv.getValue() if hasattr(pv, "getValue") else pv
            return default if val in (None, "") else val
        except Exception:
            return default

    def _iso_utc(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _to_float_or_none(self, s: Optional[str]) -> Optional[float]:
        if s is None:
            return None
        s2 = str(s).strip()
        if s2 == "":
            return None
        try:
            return float(s2)
        except Exception:
            return None

    def _csv_cell(self, v: Any, delim: str) -> str:
        s = "" if v is None else str(v)
        needs_quote = any(ch in s for ch in (delim, '"', "\n", "\r"))
        s2 = s.replace('"', '""')
        return f'"{s2}"' if needs_quote else s2

    def _route_failure(self, msg: str) -> FlowFileSourceResult:
        return FlowFileSourceResult("failure", {"mime.type": "text/plain"}, str(msg).encode("utf-8", "ignore"))

    # ---- URI helpers (aligned with your node-list processor) ----
    def _normalize_endpoint(self, url: str) -> str:
        try:
            u = urlparse((url or "").strip())
            path = (u.path or "").rstrip("/")
            host = (u.hostname or "").lower()
            scheme = (u.scheme or "").lower()
            port = f":{u.port}" if u.port else ""
            return f"{scheme}://{host}{port}{path}"
        except Exception:
            return (url or "").strip().rstrip("/").lower()

    def _sanitize_uri(self, value: str) -> str:
        if not value or not isinstance(value, str):
            return ""
        return quote(value.strip(), safe=":/@!$&'()*+,;=-._~")

    def _apply_server_uri_soft(self, client, value: str) -> None:
        uri = (value or "").strip()
        try:
            if hasattr(client, "set_server_uri"):
                client.set_server_uri(uri)
        except Exception:
            pass
        for target in (client, getattr(client, "uaclient", None)):
            try:
                if target is not None:
                    setattr(target, "server_uri", uri)
            except Exception:
                pass

    def _patch_create_session_hard(self, client, forced_uri: str) -> None:
        try:
            orig = getattr(client.uaclient, "create_session", None)
            if orig is None:
                return

            async def _create_session_patched(params, *args, **kwargs):
                try:
                    params.ServerUri = forced_uri
                except Exception:
                    pass
                return await orig(params, *args, **kwargs)

            client.uaclient.create_session = _create_session_patched  # type: ignore
        except Exception:
            pass


class _Callback:
    """
    asyncua subscription callback -> pushes to processor buffer.
    In 'client' filter mode, applies Absolute/Percent deadband at the edge.
    """
    def __init__(self, proc: OPCUASubscribe, filter_mode: str, db_abs: Optional[float], db_pct: Optional[float]):
        self.proc = proc
        self.filter_mode = (filter_mode or "client").lower()
        self.db_abs = db_abs
        self.db_pct = db_pct

    def datachange_notification(self, node, val, data):
        try:
            try:
                nid = getattr(getattr(node, "nodeid", None), "to_string", lambda: str(node))()
            except Exception:
                nid = str(node)

            src_ts = getattr(data, "source_ts", None) or getattr(data, "SourceTimestamp", None)
            iso_ts = src_ts.isoformat() if hasattr(src_ts, "isoformat") else None
            status = str(getattr(getattr(data, "status", None), "name", "")) or "Unknown"

            if self.filter_mode == "client" and (self.db_abs is not None or self.db_pct is not None):
                last = self.proc._last_sent.get(nid, None)
                if last is not None:
                    try:
                        f_new = float(val)
                        f_old = float(last)
                        delta = abs(f_new - f_old)
                        abs_ok = True if self.db_abs is None else delta >= float(self.db_abs)
                        pct_ok = True
                        if self.db_pct is not None:
                            base = abs(f_old)
                            pct_ok = (delta / base * 100.0) >= float(self.db_pct) if base != 0 else True
                        if not (abs_ok and pct_ok):
                            return
                    except Exception:
                        if val == last:
                            return

            evt = {"nodeId": nid, "value": val, "status": status, "sourceTimestamp": iso_ts}
            self.proc._append_event(evt)
            self.proc._last_sent[nid] = val

        except Exception:
            pass


# ---- Factory ----
def create(**kwargs):
    return OPCUASubscribe(**kwargs)
