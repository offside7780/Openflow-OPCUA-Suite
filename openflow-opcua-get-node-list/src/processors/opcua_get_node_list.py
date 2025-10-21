"""
OPCUAGetNodeList — Snowflake OpenFlow / NiFi Python Source Processor
Browse an OPC UA server and emit a node list (JSON/CSV/XML).

Enhancements:
- Server URI Override + Server URI Mode (auto/raw/encoded/empty)
- Auto-discover ApplicationUri from GetEndpoints (no session)
- Percent-encode spaces when needed (encoded mode)
- Hard-force CreateSession.ServerUri via patched create_session()
- Debug attributes: opcua.serverUri.used, opcua.endpoint.used
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import traceback
from dataclasses import dataclass
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
        class MessageSecurityMode:
            Sign = 1
            SignAndEncrypt = 2

        class SecurityPolicyType:
            Basic256Sha256 = object()

        class ObjectIds:
            HierarchicalReferences = 0

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

__all__ = ["OPCUAGetNodeList", "create"]

# ------------------------------------
# Data model
# ------------------------------------
@dataclass
class NodeRow:
    nodeId: str
    browseName: str
    displayName: str
    nodeClass: str


# ------------------------------------
# Processor
# ------------------------------------
class OPCUAGetNodeList(FlowFileSource):
    """Browse an OPC UA server and emit a node list as JSON/CSV/XML or write to Snowflake."""

    class Java:
        implements = ["org.apache.nifi.python.processor.FlowFileSource"]

    class ProcessorDetails:
        version = "0.2.5"
        description = "Browses an OPC UA server namespace to discover available nodes and metadata."
        tags = ["opcua", "browse", "discovery", "iiot", "uns"]
        dependencies = [
            "asyncua>=1.0.0,<2.0.0",
            "cryptography>=41,<43",
        ]

    # single-string logger wrapper for Py4J
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

    # ---------- PROPERTIES ----------
    def get_property_descriptors(self):
        try:
            non_empty = getattr(StandardValidators, "NON_EMPTY_VALIDATOR", None)
            nonneg   = getattr(StandardValidators, "NON_NEGATIVE_INTEGER_VALIDATOR", None)

            self.PROP_ENDPOINT = PropertyDescriptor(
                name="OPC UA Server Endpoint",
                description="e.g., opc.tcp://host:port/OPCUA/Server",
                required=True,
                validators=[non_empty] if non_empty else [],
                default_value="opc.tcp://69.11.87.213:53530/OPCUA/SimulationServer",
            )
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
            self.PROP_STARTNODE = PropertyDescriptor(
                name="Starting Node ID",
                description="Root NodeId to start browsing from (e.g., ns=0;i=85).",
                required=False,
                default_value="ns=0;i=85",
            )
            self.PROP_DEPTH = PropertyDescriptor(
                name="Browse Depth",
                description="Max browse depth (0=only start node).",
                required=False,
                validators=[nonneg] if nonneg else [],
                default_value="2",
            )
            self.PROP_FORMAT = PropertyDescriptor(
                name="Output Format",
                description="Output format for node list.",
                required=False,
                allowable_values=["json", "csv", "xml"],
                default_value="json",
            )
            self.PROP_TIMEOUT = PropertyDescriptor(
                name="Connect Timeout (sec)",
                description="Seconds to wait for OPC UA TCP connect/hello.",
                required=False,
                default_value="10",
            )
            self.PROP_DISCOVERY_ONLY = PropertyDescriptor(
                name="Discovery Only",
                description="If true, do not browse; just list server endpoints.",
                required=False,
                default_value="false",
            )

            return [
                self.PROP_ENDPOINT,
                self.PROP_SERVER_URI,
                self.PROP_SERVER_URI_MODE,
                self.PROP_STARTNODE,
                self.PROP_DEPTH,
                self.PROP_FORMAT,
                self.PROP_TIMEOUT,
                self.PROP_DISCOVERY_ONLY,
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
                    name="Output Format",
                    description="json|csv|xml",
                    allowable_values=["json", "csv", "xml"],
                    default_value="json",
                ),
            ]

    # camelCase alias for NiFi runtime drift
    def getPropertyDescriptors(self):
        return self.get_property_descriptors()

    # ---------- RELATIONSHIPS ----------
    REL_SUCCESS = Relationship("success", "Successfully produced node list.")
    REL_EMPTY = Relationship("empty", "No nodes matched the criteria.")
    REL_FAILURE = Relationship("failure", "Processing failed.")
    REL_SNOWFLAKE = Relationship("snowflakeWritten", "Rows written to Snowflake.")

    def get_relationships(self):
        return [self.REL_SUCCESS, self.REL_EMPTY, self.REL_FAILURE, self.REL_SNOWFLAKE]

    def getRelationships(self):
        return self.get_relationships()

    # ---------- Helpers ----------
    def _normalize_endpoint(self, url: str) -> str:
        """Normalize an endpoint URL for comparison (scheme/host/port/path, no trailing '/')."""
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
        """Percent-encode illegal URI characters but keep ':' etc. for URNs."""
        if not value or not isinstance(value, str):
            return ""
        return quote(value.strip(), safe=":/@!$&'()*+,;=-._~")

    def _looks_like_valid_uri(self, value: str) -> bool:
        """Basic acceptance for URNs and URLs."""
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
        """
        Use GetEndpoints and pick the ApplicationUri that corresponds to THIS endpoint URL.
        Falls back to the first endpoint's ApplicationUri if no direct match.
        """
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
        """Set ServerUri on the client where possible (soft)."""
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
        """
        Monkey-patch uaclient.create_session to force params.ServerUri BEFORE request is sent.
        Guarantees the server sees the exact value we intend.
        """
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

    # ---------- OPC UA browse ----------
    async def _browse_async(
        self,
        endpoint: str,
        start_id: str,
        max_depth: int,
        connect_timeout: float,
        server_uri_override: str,
        server_uri_mode: str,
    ) -> List[Dict[str, str]]:
        if Client is None:
            raise RuntimeError("asyncua is not available in this runtime (Client is None)")

        rows: List[Dict[str, str]] = []

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

        # Helper to attempt a connection with a specific ServerUri value
        async def try_connect(with_uri: str) -> bool:
            client = Client(url=endpoint, timeout=connect_timeout)
            self._apply_server_uri_soft(client, with_uri)
            self._patch_create_session_hard(client, with_uri)
            self._last_effective_server_uri = with_uri  # for attributes
            try:
                async with client:
                    start_node = client.get_node(start_id)
                    await self._walk(client, start_node, max_depth, rows)
                return True
            except Exception as e:
                s = str(e)
                # Return False on ServerUri complaints; re-raise on others
                if "BadServerUriInvalid" in s or "ServerUri" in s:
                    return False
                raise

        # Execute according to mode, with sensible fallbacks in 'auto'
        if server_uri_mode == "empty":
            if not await try_connect(""):
                raise RuntimeError("Server refused EMPTY ServerUri. Try 'raw' with the ApplicationUri from discovery.")
        elif server_uri_mode == "raw":
            # Allow raw even if not strictly URI-valid; some servers expect the literal string (with spaces).
            if not await try_connect(override_raw):
                raise RuntimeError("Server refused RAW ServerUri. Try 'encoded' mode (spaces -> %20).")
        elif server_uri_mode == "encoded":
            enc = encode_if_needed(override_raw)
            if not await try_connect(enc):
                raise RuntimeError("Server refused ENCODED ServerUri. Try 'raw' mode.")
        else:
            # AUTO: try RAW first, then ENCODED, then EMPTY
            candidate = chosen
            if candidate and await try_connect(candidate):  # RAW
                return rows
            enc = encode_if_needed(candidate)
            if enc and (enc != candidate) and await try_connect(enc):  # ENCODED
                return rows
            if await try_connect(""):  # EMPTY last
                return rows
            raise RuntimeError(
                "Server rejected RAW, ENCODED, and EMPTY ServerUri. "
                "Set 'Server URI Override' to the ApplicationUri and try 'raw' or 'encoded' mode explicitly."
            )

        return rows

    async def _walk(self, client, node, depth: int, rows: List[Dict[str, str]]):
        try:
            nid = getattr(node, "nodeid", None)
            nid_str = str(nid) if nid is not None else "<unknown>"

            browse_name = await node.read_browse_name()
            display_name = await node.read_display_name()
            node_class = await node.read_node_class()
            rows.append(
                {
                    "nodeId": nid_str,
                    "browseName": str(getattr(browse_name, "Name", browse_name)),
                    "displayName": str(getattr(display_name, "Text", display_name)),
                    "nodeClass": getattr(node_class, "name", str(node_class)),
                }
            )
        except Exception as e:
            self._log_debug(f"Read failed for node {node}: {e}")

        if depth <= 0:
            return

        try:
            children = await node.get_children()
        except Exception as e:
            self._log_debug(f"Browse failed for node {node}: {e}")
            return

        for child in children:
            await self._walk(client, child, depth - 1, rows)

    async def _discover_endpoints(self, endpoint: str, timeout: float):
        # Try with the given endpoint first; if it has a path that fails, also try host:port without the path.
        urls_to_try = [endpoint]
        try:
            u = urlparse(endpoint)
            if u.scheme and u.hostname and u.port and u.path:
                urls_to_try.append(f"{u.scheme}://{u.hostname}:{u.port}")
        except Exception:
            pass

        last_err = None
        for url in urls_to_try:
            try:
                eps = await Client(url=url, timeout=timeout).connect_and_get_server_endpoints()
                out = []
                for ep in eps:
                    out.append({
                        "endpointUrl": str(getattr(ep, "EndpointUrl", getattr(ep, "endpoint_url", ""))),
                        "securityPolicyUri": str(getattr(ep, "SecurityPolicyUri", getattr(ep, "security_policy_uri", ""))),
                        "securityMode": str(getattr(ep, "SecurityMode", getattr(ep, "security_mode", ""))),
                        "transportProfileUri": str(getattr(ep, "TransportProfileUri", getattr(ep, "transport_profile_uri",""))),
                        "userIdentityTokens": [str(getattr(t, "PolicyId", t)) for t in getattr(ep, "UserIdentityTokens", [])],
                        "applicationUri": str(getattr(getattr(ep, "Server", None), "ApplicationUri", "")) or
                                          str(getattr(getattr(ep, "server", None), "application_uri", "")) or ""
                    })
                return {"discoveryUrl": url, "endpoints": out}
            except Exception as e:
                last_err = str(e)
                continue
        raise TimeoutError(f"Endpoint discovery failed. Last error: {last_err}")

    # ---------- MAIN EXECUTION ----------
    def create(self, context):
        return self.on_trigger(context)

    def onTrigger(self, context):
        return self.on_trigger(context)

    def on_trigger(self, context):
        endpoint = None
        connect_timeout = None
        try:
            endpoint_raw = self._get(context, "OPC UA Server Endpoint")
            endpoint = (endpoint_raw or "").strip()
            server_uri_override = (self._get(context, "Server URI Override", "") or "").strip()
            server_uri_mode = (self._get(context, "Server URI Mode", "auto") or "auto").strip().lower()
            discovery_only = str(self._get(context, "Discovery Only", "false")).lower() in ("true","1","yes")
            connect_timeout = float(self._get(context, "Connect Timeout (sec)", "10"))

            if Client is None:
                msg = b"asyncua not bundled or import failed."
                self._log_error("OPCUAGetNodeList missing dependency: asyncua")
                return FlowFileSourceResult("failure", {"mime.type": "text/plain"}, msg)

            start_node_id = self._get(context, "Starting Node ID", "ns=0;i=85")
            try:
                max_depth = int(self._get(context, "Browse Depth", "2"))
            except Exception:
                max_depth = 2
            out_format = (self._get(context, "Output Format", "json") or "json").strip().lower()
            if out_format not in ("json", "csv", "xml"):
                out_format = "json"

            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                if discovery_only:
                    info = loop.run_until_complete(self._discover_endpoints(endpoint, connect_timeout))
                    data = json.dumps(info, ensure_ascii=False, indent=2).encode()
                    return FlowFileSourceResult("success", {"mime.type": "application/json"}, data)
                else:
                    rows = loop.run_until_complete(
                        self._browse_async(
                            endpoint, start_node_id, max_depth, connect_timeout,
                            server_uri_override, server_uri_mode
                        )
                    )
            finally:
                try:
                    loop.run_until_complete(asyncio.sleep(0))
                except Exception:
                    pass
                loop.close()

            if not rows:
                return FlowFileSourceResult("empty")

            # ---- serialize output ----
            if out_format == "csv":
                buf = io.StringIO()
                writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)
                data = buf.getvalue().encode("utf-8")
                mime = "text/csv"
            elif out_format == "xml":
                import xml.etree.ElementTree as ET
                root = ET.Element("nodes")
                for r in rows:
                    e = ET.SubElement(root, "node")
                    for k, v in r.items():
                        ET.SubElement(e, k).text = str(v)
                data = ET.tostring(root, encoding="utf-8")
                mime = "application/xml"
            else:
                data = json.dumps(rows, ensure_ascii=False).encode("utf-8")
                mime = "application/json"

            attrs = {"mime.type": mime}
            eff_uri = getattr(self, "_last_effective_server_uri", None)
            if eff_uri is not None:
                attrs["opcua.serverUri.used"] = str(eff_uri)
            attrs["opcua.endpoint.used"] = endpoint

            return FlowFileSourceResult("success", attrs, data)

        except (asyncio.TimeoutError, asyncio.CancelledError, TimeoutError) as e:
            msg = f"Connection to {endpoint} timed out after {connect_timeout}s.".encode()
            self._log_error(f"OPCUA connect timeout to {endpoint}: {e}")
            return FlowFileSourceResult("failure", {"mime.type": "text/plain"}, msg)

        except Exception as e:
            text = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            self._log_error(text)
            attrs = {"mime.type": "text/plain"}
            eff_uri = getattr(self, "_last_effective_server_uri", None)
            if eff_uri is not None:
                attrs["opcua.serverUri.used"] = str(eff_uri)
            if endpoint:
                attrs["opcua.endpoint.used"] = endpoint
            return FlowFileSourceResult("failure", attrs, text.encode("utf-8", "ignore"))

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
    return OPCUAGetNodeList(**kwargs)
