# opcua_get_data.py — drop-in replacement

import asyncio
import csv
import io
import json
import logging
import traceback
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, quote

# ---- NiFi / OpenFlow Python API ----
from nifiapi.flowfilesource import FlowFileSource, FlowFileSourceResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
from nifiapi.relationship import Relationship

# ---- Optional deps guarded to guarantee UI discoverability ----
try:
    from asyncua import Client, ua  # type: ignore
except Exception:  # pragma: no cover
    Client = None

    class _UAStub:
        class StatusCode:  # minimal shim for name lookup
            def __init__(self, value: int = 0):
                self.value = value
            def name(self):  # compat
                return str(self.value)

        class VariantType:
            pass

    ua = _UAStub()

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

__all__ = ["OPCUAGetData", "create"]


# ------------------------------------
# Data model
# ------------------------------------
@dataclass
class DataRow:
    nodeId: str
    value: Any
    dataType: Optional[str]
    status: Optional[int]
    statusName: Optional[str]
    sourceTimestamp: Optional[str]
    serverTimestamp: Optional[str]


# ------------------------------------
# Processor
# ------------------------------------
class OPCUAGetData(FlowFileSource):
    """
    Read current data values for one or more OPC UA NodeIds and emit JSON/CSV, or write to Snowflake.
    """

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileSource"]

    class ProcessorDetails:
        version = "1.0.1"
        description = "Reads current values (value, timestamps, quality) for a list of OPC UA NodeIds."
        tags = ["opcua", "read", "data", "iiot", "uns"]
        dependencies = [
            "asyncua>=1.0.0,<2.0.0",
            "cryptography>=41,<43",
            # Optional for Snowflake path:
            "pandas>=2,<3",
            "pyarrow>=14,<19",
            "snowflake-connector-python>=3,<5",
        ]

    # ---- tiny log helpers (Py4J sometimes hates complex args) ----
    def _log_error(self, message: str) -> None:
        try:
            self.logger.error(str(message))
        except Exception:
            pass

    def _log_debug(self, message: str) -> None:
        try:
            self.logger.debug(str(message))
        except Exception:
            pass

    def __init__(self, **kwargs):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        self._last_effective_server_uri = None

    # ---------- PROPERTIES ----------
    def get_property_descriptors(self):
        try:
            non_empty = getattr(StandardValidators, "NON_EMPTY_VALIDATOR", None)
            nonneg = getattr(StandardValidators, "NON_NEGATIVE_INTEGER_VALIDATOR", None)

            self.PROP_ENDPOINT = PropertyDescriptor(
                name="OPC UA Server Endpoint",
                description="e.g., opc.tcp://host:port/OPCUA/Server",
                required=True,
                validators=[non_empty] if non_empty else [],
                default_value="",
            )
            # NEW: match get-node-list
            self.PROP_SERVER_URI = PropertyDescriptor(
                name="Server URI Override",
                description=("Exact ApplicationUri to send in CreateSession.ServerUri. "
                             "If blank, will try to auto-discover from GetEndpoints. "
                             "If your ApplicationUri has spaces, consider 'encoded' mode."),
                required=False,
                default_value="",
            )
            self.PROP_SERVER_URI_MODE = PropertyDescriptor(
                name="Server URI Mode",
                description=("How to send ServerUri in CreateSession: "
                             "auto (try raw, then encoded, then empty), "
                             "raw (send exactly what you typed), "
                             "encoded (percent-encode spaces), "
                             "empty (send empty string)."),
                required=False,
                default_value="auto",
                allowable_values=["auto", "raw", "encoded", "empty"],
            )

            self.PROP_NODEIDS = PropertyDescriptor(
                name="Node IDs",
                description="Comma/newline-separated list of NodeIds to read (e.g., ns=2;s=Demo.Static.Scalar.Int32).",
                required=True,
                validators=[non_empty] if non_empty else [],
            )
            self.PROP_TIMEOUT = PropertyDescriptor(
                name="Connect Timeout (sec)",
                description="Seconds to wait for OPC UA TCP connect/hello.",
                required=False,
                default_value="10",
            )
            self.PROP_READ_TIMEOUT = PropertyDescriptor(
                name="Per-Read Timeout (sec)",
                description="Optional timeout for each node read (0 = no extra timeout).",
                required=False,
                default_value="0",
            )
            self.PROP_FORMAT = PropertyDescriptor(
                name="Output Format",
                description="Output format for data rows.",
                required=False,
                allowable_values=["json", "csv"],
                default_value="json",
            )
            self.PROP_INCLUDE_STATUS = PropertyDescriptor(
                name="Include Status/Quality",
                description="Include StatusCode and name in the output.",
                required=False,
                default_value="true",
            )
            self.PROP_INCLUDE_TIMESTAMPS = PropertyDescriptor(
                name="Include Timestamps",
                description="Include SourceTimestamp and ServerTimestamp in the output.",
                required=False,
                default_value="true",
            )

            # ---- Optional Snowflake sink ----
            self.PROP_SF_ENABLE = PropertyDescriptor(
                name="Snowflake: Enable Write",
                description="If true, write rows to Snowflake (also emits success flow file).",
                required=False,
                default_value="false",
            )
            self.PROP_SF_TABLE = PropertyDescriptor(
                name="Snowflake: Table",
                description="Fully-qualified table (DATABASE.SCHEMA.TABLE) or just TABLE (use DB/SCHEMA from params).",
                required=False,
                default_value="OPCUA_DATA",
            )
            self.PROP_SF_PARAMS_JSON = PropertyDescriptor(
                name="Snowflake: Connection JSON",
                description=(
                    "JSON of connect params, e.g. "
                    '{"account":"ACCT","user":"USER","password":"***","warehouse":"WH","database":"DB","schema":"SCHEMA","role":"ROLE"}'
                ),
                required=False,
            )

            return [
                self.PROP_ENDPOINT,
                self.PROP_SERVER_URI,
                self.PROP_SERVER_URI_MODE,
                self.PROP_NODEIDS,
                self.PROP_TIMEOUT,
                self.PROP_READ_TIMEOUT,
                self.PROP_FORMAT,
                self.PROP_INCLUDE_STATUS,
                self.PROP_INCLUDE_TIMESTAMPS,
                self.PROP_SF_ENABLE,
                self.PROP_SF_TABLE,
                self.PROP_SF_PARAMS_JSON,
            ]
        except Exception as e:
            self._log_error(
                f"get_property_descriptors failed; returning minimal set: {e}\n{traceback.format_exc()}"
            )
            return [
                PropertyDescriptor(
                    name="OPC UA Server Endpoint",
                    description="e.g., opc.tcp://host:port",
                    required=True,
                ),
                PropertyDescriptor(
                    name="Node IDs",
                    description="Comma/newline separated NodeIds to read.",
                    required=True,
                ),
                PropertyDescriptor(
                    name="Output Format",
                    description="json|csv",
                    allowable_values=["json", "csv"],
                    default_value="json",
                ),
            ]

    # camelCase alias for NiFi runtime drift
    def getPropertyDescriptors(self):
        return self.get_property_descriptors()

    # ---------- RELATIONSHIPS ----------
    REL_SUCCESS = Relationship("success", "Successfully produced data.")
    REL_EMPTY = Relationship("empty", "No rows produced.")
    REL_FAILURE = Relationship("failure", "Processing failed.")
    REL_SNOWFLAKE = Relationship("snowflakeWritten", "Rows written to Snowflake.")

    def get_relationships(self):
        return [self.REL_SUCCESS, self.REL_EMPTY, self.REL_FAILURE, self.REL_SNOWFLAKE]

    def getRelationships(self):
        return self.get_relationships()

    # ---------- Helpers (parity with get-node-list) ----------
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

    def _looks_like_valid_uri(self, value: str) -> bool:
        if not value or not isinstance(value, str):
            return False
        v = value.strip()
        if v.lower().startswith("urn:"):
            return True
        try:
            u = urlparse(v)
            return bool(u.scheme) and (bool(u.netloc) or bool(u.path))
        except Exception:
            return False

    async def _discover_application_uri_for_url(self, endpoint: str, timeout: float) -> Optional[str]:
        if Client is None:
            return None
        try:
            target = self._normalize_endpoint(endpoint)
            eps = await Client(url=endpoint, timeout=timeout).connect_and_get_server_endpoints()
            best = None
            for ep in eps:
                ep_url = str(getattr(ep, "EndpointUrl", getattr(ep, "endpoint_url", "")))
                ep_norm = self._normalize_endpoint(ep_url)
                server_desc = getattr(ep, "Server", getattr(ep, "server", None))
                app_uri = None
                if server_desc is not None:
                    app_uri = getattr(server_desc, "ApplicationUri", getattr(server_desc, "application_uri", None))
                if ep_norm == target and app_uri:
                    return str(app_uri)
                if best is None and app_uri:
                    best = str(app_uri)
            return best
        except Exception as e:
            self._log_debug(f"GetEndpoints ApplicationUri discovery failed: {e}")
            return None

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
        except Exception as e:
            self._log_debug(f"create_session patch failed: {e}")

    # ---------- Helpers (existing behavior) ----------
    def _parse_nodeids(self, raw: str) -> List[str]:
        if not raw:
            return []
        parts = [x.strip() for x in raw.replace("\r", "\n").replace(",", "\n").split("\n")]
        return [p for p in parts if p]

    def _bool(self, s: Optional[str], default: bool = False) -> bool:
        if s is None:
            return default
        return str(s).strip().lower() in ("true", "1", "yes", "y")

    def _serialize_value(self, val: Any) -> Any:
        try:
            if isinstance(val, (bytes, bytearray)):
                return val.hex()
            if isinstance(val, (dict, list, tuple, str, int, float, bool)) or val is None:
                return val
            return str(val)
        except Exception:
            return str(val)

    # ---------- Async read with ServerUri controls ----------
    async def _read_values_async(
        self,
        endpoint: str,
        node_ids: List[str],
        connect_timeout: float,
        per_read_timeout: float,
        include_status: bool,
        include_timestamps: bool,
        server_uri_override: str,
        server_uri_mode: str,
    ) -> List[Dict[str, Any]]:
        if Client is None:
            raise RuntimeError("asyncua is not available in this runtime (Client is None)")

        rows: List[Dict[str, Any]] = []

        override_raw = (server_uri_override or "").strip()

        def encode_if_needed(v: str) -> str:
            return self._sanitize_uri(v)

        async def pick_auto() -> str:
            # Prefer override as-is; else discover
            candidate = override_raw or (await self._discover_application_uri_for_url(endpoint, connect_timeout) or "")
            return candidate.strip()

        # Choose initial candidate based on mode
        if server_uri_mode == "empty":
            chosen = ""
        elif server_uri_mode == "raw":
            chosen = override_raw
        elif server_uri_mode == "encoded":
            chosen = encode_if_needed(override_raw)
        else:  # auto
            chosen = await pick_auto()

        async def try_connect(with_uri: str) -> Optional[Client]:
            client = Client(url=endpoint, timeout=connect_timeout)
            self._apply_server_uri_soft(client, with_uri)
            self._patch_create_session_hard(client, with_uri)
            self._last_effective_server_uri = with_uri
            try:
                await client.connect()
                return client
            except Exception as e:
                s = str(e)
                if "BadServerUriInvalid" in s or "ServerUri" in s:
                    return None
                raise

        # Execute according to mode with fallbacks for 'auto'
        client = None
        if server_uri_mode == "empty":
            client = await try_connect("")
            if client is None:
                raise RuntimeError("Server refused EMPTY ServerUri. Try 'raw' with the ApplicationUri from discovery.")
        elif server_uri_mode == "raw":
            client = await try_connect(override_raw)
            if client is None:
                raise RuntimeError("Server refused RAW ServerUri. Try 'encoded' mode (spaces -> %20).")
        elif server_uri_mode == "encoded":
            enc = encode_if_needed(override_raw)
            client = await try_connect(enc)
            if client is None:
                raise RuntimeError("Server refused ENCODED ServerUri. Try 'raw' mode.")
        else:
            candidate = chosen
            client = await try_connect(candidate) if candidate else None
            if client is None:
                enc = encode_if_needed(candidate) if candidate else ""
                client = await try_connect(enc) if enc and (enc != candidate) else None
            if client is None:
                client = await try_connect("")
            if client is None:
                raise RuntimeError(
                    "Server rejected RAW, ENCODED, and EMPTY ServerUri. "
                    "Set 'Server URI Override' to the ApplicationUri and try 'raw' or 'encoded' explicitly."
                )

        # Connected: perform reads
        try:
            async with client:
                for nid in node_ids:
                    node = client.get_node(nid)
                    try:
                        if per_read_timeout and per_read_timeout > 0:
                            dv = await asyncio.wait_for(node.read_data_value(), timeout=per_read_timeout)
                        else:
                            dv = await node.read_data_value()

                        value = getattr(getattr(dv, "Value", None), "Value", None)
                        s_ts = getattr(dv, "SourceTimestamp", None)
                        v_s_ts = s_ts.isoformat() if hasattr(s_ts, "isoformat") else (str(s_ts) if s_ts else None)
                        srv_ts = getattr(dv, "ServerTimestamp", None)
                        v_srv_ts = srv_ts.isoformat() if hasattr(srv_ts, "isoformat") else (str(srv_ts) if srv_ts else None)
                        sc = getattr(dv, "StatusCode", None)
                        sc_val = getattr(sc, "value", None)
                        sc_name = None
                        try:
                            sc_name = sc.name  # asyncua often exposes .name attr
                        except Exception:
                            sc_name = str(sc) if sc is not None else None

                        dt_name = None
                        try:
                            v = getattr(dv, "Value", None)
                            if v is not None:
                                dt = getattr(v, "VariantType", None) or getattr(v, "varianttype", None)
                                dt_name = getattr(dt, "name", None) or (str(dt) if dt else None)
                        except Exception:
                            dt_name = None

                        record = {
                            "nodeId": nid,
                            "value": self._serialize_value(value),
                            "dataType": dt_name,
                        }
                        if include_status:
                            record["status"] = sc_val
                            record["statusName"] = sc_name
                        if include_timestamps:
                            record["sourceTimestamp"] = v_s_ts
                            record["serverTimestamp"] = v_srv_ts

                        rows.append(record)

                    except asyncio.TimeoutError:
                        rows.append(
                            {
                                "nodeId": nid,
                                "value": None,
                                "dataType": None,
                                "status": 0,
                                "statusName": "ReadTimeout",
                                "sourceTimestamp": None,
                                "serverTimestamp": None,
                            }
                        )
                    except Exception as e:
                        self._log_debug(f"Read failed for {nid}: {e}")
                        rows.append(
                            {
                                "nodeId": nid,
                                "value": None,
                                "dataType": None,
                                "status": 0,
                                "statusName": f"ReadError:{type(e).__name__}",
                                "sourceTimestamp": None,
                                "serverTimestamp": None,
                            }
                        )
        finally:
            try:
                await asyncio.sleep(0)
            except Exception:
                pass

        return rows

    # ---------- MAIN EXECUTION ----------
    def create(self, context):
        return self.on_trigger(context)

    def onTrigger(self, context):
        return self.on_trigger(context)

    def on_trigger(self, context):
        endpoint = None
        connect_timeout = None
        try:
            endpoint = (self._get(context, "OPC UA Server Endpoint") or "").strip()
            server_uri_override = (self._get(context, "Server URI Override", "") or "").strip()
            server_uri_mode = (self._get(context, "Server URI Mode", "auto") or "auto").strip().lower()

            nodeids_raw = self._get(context, "Node IDs", "")
            connect_timeout = float(self._get(context, "Connect Timeout (sec)", "10"))
            per_read_timeout = float(self._get(context, "Per-Read Timeout (sec)", "0"))
            out_format = str(self._get(context, "Output Format", "json")).lower()
            include_status = self._bool(self._get(context, "Include Status/Quality", "true"), True)
            include_ts = self._bool(self._get(context, "Include Timestamps", "true"), True)

            sf_enable = self._bool(self._get(context, "Snowflake: Enable Write", "false"), False)
            sf_table = self._get(context, "Snowflake: Table", "OPCUA_DATA")
            sf_params_json = self._get(context, "Snowflake: Connection JSON", "")

            if Client is None:
                msg = b"asyncua not bundled or import failed."
                self._log_error("OPCUAGetData missing dependency: asyncua")
                return FlowFileSourceResult("failure", {"mime.type": "text/plain"}, msg)

            node_ids = self._parse_nodeids(nodeids_raw)
            if not node_ids:
                return FlowFileSourceResult(
                    "empty",
                    {"mime.type": "text/plain", "opcua.endpoint.used": endpoint},
                    b"No NodeIds provided.",
                )

            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                rows = loop.run_until_complete(
                    self._read_values_async(
                        endpoint=endpoint,
                        node_ids=node_ids,
                        connect_timeout=connect_timeout,
                        per_read_timeout=per_read_timeout,
                        include_status=include_status,
                        include_timestamps=include_ts,
                        server_uri_override=server_uri_override,
                        server_uri_mode=server_uri_mode,
                    )
                )
            finally:
                try:
                    loop.run_until_complete(asyncio.sleep(0))
                except Exception:
                    pass
                loop.close()

            if not rows:
                attrs = {"opcua.endpoint.used": endpoint}
                eff_uri = getattr(self, "_last_effective_server_uri", None)
                if eff_uri is not None:
                    attrs["opcua.serverUri.used"] = str(eff_uri)
                return FlowFileSourceResult("empty", attrs)

            # ---- Optional Snowflake write ----
            wrote_sf = False
            if sf_enable and pd is not None and sf_connect is not None and write_pandas is not None:
                try:
                    params = {}
                    if sf_params_json:
                        try:
                            params = json.loads(sf_params_json)
                        except Exception as e:
                            self._log_error(f"Invalid Snowflake params JSON: {e}")
                    df = pd.DataFrame(rows)

                    db = params.get("database")
                    schema = params.get("schema")
                    table_name = sf_table
                    parts = str(sf_table).split(".")
                    if len(parts) == 3:
                        db, schema, table_name = parts[0], parts[1], parts[2]

                    conn = sf_connect(**{k: v for k, v in params.items() if v not in (None, "")})
                    try:
                        cur = conn.cursor()
                        try:
                            if db:
                                cur.execute(f'CREATE DATABASE IF NOT EXISTS "{db}"')
                            if db and schema:
                                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{db}"."{schema}"')
                        finally:
                            cur.close()

                        write_pandas(conn, df, table_name, database=db, schema=schema, quote_identifiers=True)
                        wrote_sf = True
                    finally:
                        try:
                            conn.close()
                        except Exception:
                            pass
                except Exception as e:
                    self._log_error(f"Snowflake write failed: {e}\n{traceback.format_exc()}")

            # ---- Emit FlowFile data ----
            if out_format == "csv":
                buf = io.StringIO()
                fieldnames = list(rows[0].keys())
                writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
                data = buf.getvalue().encode("utf-8")
                mime = "text/csv"
            else:
                data = json.dumps(rows, ensure_ascii=False).encode("utf-8")
                mime = "application/json"

            attrs = {"mime.type": mime, "opcua.endpoint.used": endpoint}
            eff_uri = getattr(self, "_last_effective_server_uri", None)
            if eff_uri is not None:
                attrs["opcua.serverUri.used"] = str(eff_uri)

            rel = "snowflakeWritten" if wrote_sf else "success"
            return FlowFileSourceResult(rel, attrs, data)

        except (asyncio.TimeoutError, asyncio.CancelledError, TimeoutError) as e:
            msg = f"Connection to {endpoint} timed out after {connect_timeout}s.".encode()
            self._log_error(f"OPCUA connect timeout to {endpoint}: {e}")
            attrs = {"mime.type": "text/plain"}
            if endpoint:
                attrs["opcua.endpoint.used"] = endpoint
            eff_uri = getattr(self, "_last_effective_server_uri", None)
            if eff_uri is not None:
                attrs["opcua.serverUri.used"] = str(eff_uri)
            return FlowFileSourceResult("failure", attrs, msg)

        except Exception as e:
            text = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            self._log_error(text)
            attrs = {"mime.type": "text/plain"}
            eff_uri = getattr(self, "_last_effective_server_uri", None)
            if eff_uri is not None:
                attrs["opcua.serverUri.used"] = str(eff_uri)
            if endpoint:
                attrs["opcua.endpoint.used"] = endpoint
            return FlowFileSourceResult(
                "failure", attrs, text.encode("utf-8", "ignore")
            )

    # ---------- UTILITIES ----------
    def _get(self, context, name, default=None):
        pv = context.getProperty(name)
        if pv is None:
            return default
        try:
            if hasattr(pv, "evaluateAttributeExpressions"):
                pv = pv.evaluateAttributeExpressions()
            val = pv.getValue() if hasattr(pv, "getValue") else pv
        except Exception:
            val = str(pv)
        return default if val in (None, "") else val


# ---- Factory for NiFi loader ----
def create(**kwargs):
    return OPCUAGetData(**kwargs)
