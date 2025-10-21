🐉 OPCUAGetData — Snowflake OpenFlow / NiFi Python Source Processor

Version: 1.0.0
Namespace: net.ksmcloud.opcua.processors.OPCUAGetData
Author: Kevin Mahnke
License: MIT

🧩 Overview

OPCUAGetData is a custom OpenFlow / Apache NiFi Python Source Processor that reads current OPC UA node values and emits results as JSON or CSV flow files — optionally writing directly to Snowflake for real-time data ingestion into your Unified Namespace (UNS).

This processor is designed for:

Industry 4.0 / IIoT data acquisition

OPC UA → Snowflake integration pipelines

Low-latency digital twin feeds

Unified Namespace (UNS) population

It safely supports Matrikon UA Tunneller, raw or encoded ServerUris, and zero-dependency UI discoverability, making it ideal for hybrid industrial deployments.

⚙️ Features
Capability	Description
🔌 OPC UA Data Reads	Reads current values, status, and timestamps for multiple NodeIds.
🧠 Server URI Override & Modes	Supports auto, raw, encoded, and empty modes for OPC UA CreateSession compatibility.
🧾 JSON / CSV Output	Outputs structured JSON arrays or CSV tables for easy downstream processing.
❄️ Optional Snowflake Sink	Writes results directly to a Snowflake table using write_pandas().
🧱 UI Discoverability	All properties and relationships are visible in the OpenFlow canvas, even if asyncua is missing.
🧰 Dependency-guarded Imports	AsyncUA, pandas, pyarrow, and Snowflake connector are optional and safely wrapped.
🛡️ Compatible with ISA-95 UNS Designs	Provides clean, consistent OPC UA tag structures suitable for hierarchical UNS schemas.
📦 Dependencies

The following dependencies are used but guarded — the processor still loads in the OpenFlow UI even if they’re missing:

asyncua>=1.0.0,<2.0.0
cryptography>=41,<43
pandas>=2,<3
pyarrow>=14,<19
snowflake-connector-python>=3,<5


✅ Only asyncua and cryptography are required for OPC UA connectivity.
🧊 Snowflake support (optional) requires pandas, pyarrow, and snowflake-connector-python.

🧠 Properties
Property	Description	Default
OPC UA Server Endpoint	Full endpoint URL (e.g. opc.tcp://host:port/OPCUA/Server).	opc.tcp://69.11.87.213:53530/OPCUA/SimulationServer
Server URI Override	Exact ApplicationUri for CreateSession.ServerUri. Leave blank for auto-discovery.	(empty)
Server URI Mode	How to apply ServerUri — auto, raw, encoded, or empty.	auto
Node IDs	Comma / newline separated list of NodeIds to read.	(required)
Connect Timeout (sec)	OPC UA TCP connect/hello timeout.	10
Per-Read Timeout (sec)	Optional timeout for each node read (0 = no timeout).	0
Output Format	json or csv.	json
Include Status/Quality	Include OPC UA StatusCode and name in output.	true
Include Timestamps	Include SourceTimestamp and ServerTimestamp in output.	true
Snowflake: Enable Write	If true, writes rows to Snowflake (in addition to emitting success flow file).	false
Snowflake: Table	Fully-qualified table name (DB.SCHEMA.TABLE) or just TABLE.	OPCUA_DATA
Snowflake: Connection JSON	JSON parameters for connect(), e.g. {"account":"ACCT","user":"USER","password":"***","database":"DB","schema":"SCHEMA"}	(empty)
🔀 Relationships
Relationship	Description
success	Successfully produced OPC UA data.
empty	No rows produced (no NodeIds or no data).
failure	Connection or read error occurred.
snowflakeWritten	Data written to Snowflake successfully.
🧪 Example Output

JSON example:

[
  {
    "nodeId": "ns=2;s=Demo.Static.Scalar.Float",
    "value": 12.34,
    "dataType": "Float",
    "status": 0,
    "statusName": "Good",
    "sourceTimestamp": "2025-10-21T06:00:00Z",
    "serverTimestamp": "2025-10-21T06:00:00Z"
  }
]


CSV example:

nodeId,value,dataType,status,statusName,sourceTimestamp,serverTimestamp
ns=2;s=Demo.Static.Scalar.Float,12.34,Float,0,Good,2025-10-21T06:00:00Z,2025-10-21T06:00:00Z

🧊 Snowflake Integration

To enable direct Snowflake ingestion:

Set Snowflake: Enable Write → true

Provide Snowflake: Connection JSON, e.g.:

{
  "account": "ACCT",
  "user": "USER",
  "password": "****",
  "warehouse": "COMPUTE_WH",
  "database": "OPCUA_DB",
  "schema": "PUBLIC",
  "role": "SYSADMIN"
}


Optionally set Snowflake: Table to OPCUA_DB.PUBLIC.OPCUA_DATA.
Tables and schemas will be auto-created if they don’t exist.

🏗️ Build & Deploy
Hatch / hatch-datavolo-nar configuration:
[tool.hatch.build.targets.nar]
sources = ["src"]
packages = ["processors"]

entry_points = [
  "net.ksmcloud.opcua.processors.OPCUAGetData = processors.opcua_get_data:OPCUAGetData"
]

Directory Structure
src/
 └── processors/
      └── opcua_get_data.py


Build your NAR:

hatch build -t nar


Deploy to your Snowflake OpenFlow / NiFi instance by placing the generated
openflow-opcua-get-data-nar-<version>.nar into:

/opt/runtime/extensions/


Restart the runtime to load the new processor.

🧭 Usage Example (OpenFlow Canvas)

Add Processor → OPCUAGetData

Configure:

OPC UA Server Endpoint: opc.tcp://93.92.30.18:21381/MatrikonOpcUaWrapper

Node IDs:

ns=2;s=Compressor.Pressure
ns=2;s=Compressor.Temperature


Server URI Mode: auto

Output Format: json

Connect success → PutSnowflake or PutFile

Run flow; verify JSON payloads or Snowflake records.

🧱 Integration Pattern

Typical UNS Pipeline:

[ OPCUAGetData ]
       ↓
  [ Transform / Route ]
       ↓
  [ PutSnowflake ]


Supports integration into multi-tier UNS models
(raw → bronze → silver → gold) for real-time industrial telemetry.

🧰 Troubleshooting
Symptom	Likely Cause / Fix
BadServerUriInvalid	Try setting Server URI Mode to encoded or raw with the server’s ApplicationUri.
No rows emitted	Check Node IDs formatting or verify server connectivity.
UI processor missing	Verify __all__ = ["OPCUAGetData","create"] and FlowFileSource inheritance.
Snowflake write failed	Confirm JSON credentials and network egress to Snowflake.
🧾 License

MIT License © 2025
Use freely in commercial and industrial applications.
