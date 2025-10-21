🐉 OPCUAGetNodeList — Snowflake OpenFlow / NiFi Python Source Processor

Version: 0.2.5
Namespace: net.ksmcloud.opcua.processors.OPCUAGetNodeList
Author: Kevin Mahnke
License: MIT

🧩 Overview

OPCUAGetNodeList is a custom OpenFlow / Apache NiFi Python Source Processor that browses an OPC UA server’s namespace and emits a structured node list (in JSON, CSV, or XML) suitable for building industrial metadata catalogs or Unified Namespace (UNS) models.

It is optimized for:

OPC UA namespace discovery

UNS model generation and tag lineage tracking

OPC UA ↔ Snowflake integration pipelines

OPC DA tunneller compatibility (Matrikon UA Wrapper)

Seamless UI discoverability and OpenFlow runtime stability

⚙️ Features
Capability	Description
🌐 Namespace Browsing	Recursively explores OPC UA server nodes (Object, Variable, Method, etc.) up to configurable depth.
🔍 Server URI Override + Mode Control	Allows explicit ServerUri in CreateSession with auto, raw, encoded, and empty options.
🧠 ApplicationUri Auto-Discovery	Uses GetEndpoints to automatically find the correct ApplicationUri — no session required.
🧾 Flexible Output	Emits JSON, CSV, or XML formats for downstream consumption or Snowflake ingestion.
🧊 Snowflake-ready Metadata	Optional integration path to Snowflake for UNS bronze/silver modeling.
🧰 Import-safe Design	All optional dependencies are guarded; processor remains visible in UI even without asyncua.
🧱 Industrial Standards Alignment	Compatible with ISA-95 / UNS hierarchical tagging conventions.
📦 Dependencies

The following dependencies are declared (optional at runtime):

asyncua>=1.0.0,<2.0.0
cryptography>=41,<43
pandas>=2,<3
pyarrow>=14,<19
snowflake-connector-python>=3,<5


✅ Only asyncua + cryptography are needed for browsing.
🧊 pandas, pyarrow, and snowflake-connector-python are optional for Snowflake export or format conversions.

🧠 Properties
Property	Description	Default
OPC UA Server Endpoint	Full endpoint URL, e.g. opc.tcp://host:port/OPCUA/Server.	opc.tcp://69.11.87.213:53530/OPCUA/SimulationServer
Server URI Override	Exact ApplicationUri to use in CreateSession.ServerUri. Leave blank for auto-discovery.	(empty)
Server URI Mode	How to set the ServerUri — auto, raw, encoded, or empty.	auto
Starting Node ID	Root NodeId to start browsing from.	ns=0;i=85
Browse Depth	Maximum recursive browse depth (0 = only start node).	2
Output Format	Output format — json, csv, or xml.	json
Connect Timeout (sec)	Seconds to wait for OPC UA TCP connect/hello.	10
Discovery Only	If true, list server endpoints instead of browsing the namespace.	false
🔀 Relationships
Relationship	Description
success	Node list or endpoint discovery completed successfully.
empty	No nodes matched the criteria (e.g., empty branch).
failure	OPC UA connection, browse, or serialization failed.
snowflakeWritten	Node metadata written to Snowflake (optional future use).
🧾 Output Examples
JSON
[
  {
    "nodeId": "ns=2;s=Demo.Static.Scalar.Int32",
    "browseName": "Int32",
    "displayName": "Int32",
    "nodeClass": "Variable"
  },
  {
    "nodeId": "ns=2;s=Demo.Static.Scalar.Float",
    "browseName": "Float",
    "displayName": "Float",
    "nodeClass": "Variable"
  }
]

CSV
nodeId,browseName,displayName,nodeClass
ns=2;s=Demo.Static.Scalar.Int32,Int32,Int32,Variable
ns=2;s=Demo.Static.Scalar.Float,Float,Float,Variable

XML
<nodes>
  <node>
    <nodeId>ns=2;s=Demo.Static.Scalar.Int32</nodeId>
    <browseName>Int32</browseName>
    <displayName>Int32</displayName>
    <nodeClass>Variable</nodeClass>
  </node>
</nodes>

🧊 Example Snowflake Workflow

Processor: OPCUAGetNodeList

Downstream: Transform / Flatten JSON → PutSnowflake

Result: OPCUA_NODELIST table containing tag metadata and class hierarchy

Example schema:

Column	Type	Description
nodeId	STRING	Fully-qualified OPC UA NodeId
browseName	STRING	Node’s browse name
displayName	STRING	User-friendly display name
nodeClass	STRING	Node type (Variable/Object/Method)
🏗️ Build & Deploy
Hatch / hatch-datavolo-nar
[tool.hatch.build.targets.nar]
sources = ["src"]
packages = ["processors"]

entry_points = [
  "net.ksmcloud.opcua.processors.OPCUAGetNodeList = processors.opcua_get_node_list:OPCUAGetNodeList"
]

Project Layout
src/
 └── processors/
      └── opcua_get_node_list.py

Build Command
hatch build -t nar

Deploy to OpenFlow

Place the generated NAR file into:

/opt/runtime/extensions/


Restart the OpenFlow runtime to register the new processor.

🧭 Usage Example (Canvas)

Add Processor: OPCUAGetNodeList

Set Properties:

OPC UA Server Endpoint: opc.tcp://93.92.30.18:21381/MatrikonOpcUaWrapper

Browse Depth: 2

Output Format: json

Connect Output: → PutFile or PutSnowflake

Run Flow: observe node hierarchy in FlowFile contents.

🧱 Typical UNS Pattern
[ OPCUAGetNodeList ]
       ↓
[ Transform Metadata ]
       ↓
[ Snowflake UNS Bronze ]


Integrate this with OPCUAGetData and OPCUASubscribe to build a full UNS ingestion pipeline.

🧰 Troubleshooting
Symptom	Likely Cause / Fix
BadServerUriInvalid	Switch Server URI Mode to encoded or raw.
No nodes returned	Increase Browse Depth or verify starting NodeId.
UI not showing processor	Ensure __all__ includes ["OPCUAGetNodeList","create"] and correct class inheritance.
Timeouts / no connection	Check firewall/NAT for TCP port (usually 4840 or custom).
XML encoding errors	Verify no non-UTF8 characters in displayName.
🧾 License

MIT License ©
Free for industrial and commercial use.
