🐉 OPC UA Processor Suite
for Snowflake OpenFlow / Apache NiFi (Python NAR Extensions)

Version: 2025.10
Namespace: net.ksmcloud.opcua.processors.*
Author: Kevin Mahnke
License: MIT

🧩 Overview

The OPC UA Processor Suite provides a complete set of custom OpenFlow / Apache NiFi Python processors for integrating industrial OPC UA data directly into Snowflake or any downstream data pipeline.

Built for Industry 4.0, Unified Namespace (UNS), and IIoT edge connectivity, these processors deliver full coverage from metadata discovery and historical reads to real-time subscriptions and command writes — with asyncua-based OPC UA client support and Snowflake-ready output.

This suite forms the core of a UNS ingestion and governance layer for oil & gas, manufacturing, and industrial automation systems.

⚙️ Processors Included
Processor	Purpose	Typical Use
🧭 OPCUAGetNodeData	Browse OPC UA servers and discover node hierarchies (metadata).	Building UNS model layers, tag lineage, and data catalogs.
📈 OPCUAGetData	Read current values from one or more OPC UA NodeIds.	One-shot tag polling or scheduled asset snapshots.
🕰️ OPCUAHistoryRead	Retrieve time-series history from servers that support OPC UA Historical Access (HA).	Backfilling Snowflake tables with time-aligned historical values.
🔄 OPCUASubscribe	Continuous live subscription to NodeIds with client/server deadband filters.	Real-time telemetry streaming into UNS or Snowflake bronze.
✍️ OPCUAWriteData	Write values back to OPC UA nodes.	Closed-loop control, digital twin updates, or remote overrides.
🧠 Design Goals
Goal	Description
🧱 Modular	Each processor is standalone yet interoperable — you can chain them seamlessly.
🧰 Discoverable	All processors are fully visible in the OpenFlow UI even without optional dependencies.
🔒 Secure	Supports secure sessions, credentials, and Snowflake connector encryption.
🧠 Smart Server URI Handling	Unified logic for ServerUri auto-discovery, override, and encoded/raw/empty modes.
❄️ Snowflake-Native Output	Each processor can emit directly to Snowflake or as JSON/CSV/XML flow files.
🧾 Standards-Aligned	Designed around ISA-95, OPC UA Part 3, and Unified Namespace concepts.
⚙️ Minimal Dependencies	Only asyncua + cryptography are required; others are optional for analytics/sinks.
🧩 Architecture Overview
 ┌────────────────────────────────────────────────────────────────┐
 │                       OPC UA Processor Suite                    │
 │                                                                │
 │  [ OPCUAGetNodeData ] → [ OPCUAGetData ] → [ OPCUASubscribe ]  │
 │               │                    │                │           │
 │          Metadata          Current Values       Real-time       │
 │                                                                │
 │  [ OPCUAHistoryRead ]  ←→  [ OPCUAWriteData ]                   │
 │     Historical Backfill       Command / Control                 │
 └────────────────────────────────────────────────────────────────┘

 →  All outputs compatible with Snowflake, JSON, CSV, or XML sinks.
 →  Ideal for building multi-tier UNS models: raw → bronze → silver → gold.

🧾 Output Formats

All processors support consistent serialization modes:

Format	Description	MIME Type
JSON	Default structured payload for flowfile transmission.	application/json
CSV	Tabular row format with optional header.	text/csv
XML	Hierarchical data exchange format.	application/xml
Snowflake Sink	Writes directly into Snowflake tables using write_pandas.	application/json (summary)
🧊 Snowflake Integration

All processors optionally support direct Snowflake writes with auto-schema creation.
Typical schema for data tables:

Column	Type	Description
NODE_ID	STRING	OPC UA NodeId
VALUE	VARIANT	OPC UA value (numeric, string, boolean, etc.)
STATUS	STRING	OPC UA StatusCode name
SOURCE_TS	TIMESTAMP_TZ	Source timestamp (from device/server)
ENDPOINT	STRING	OPC UA server endpoint
INGEST_TS	TIMESTAMP_TZ	Time of ingestion into Snowflake

Configuration fields:

SF Account     = <account>.snowflakecomputing.com
SF User        = <user>
SF Password    = <password>
SF Warehouse   = COMPUTE_WH
SF Database    = OPCUA_DB
SF Schema      = PUBLIC
SF Table       = OPCUA_EVENTS
SF Auto-Create Table = true

🧱 Example UNS Data Flow

Level 1 – Metadata Discovery

[OPCUAGetNodeData] → [Transform → PutSnowflake]
→ Produces tag hierarchies and class metadata.


Level 2 – Snapshot Values

[OPCUAGetData] → [PutSnowflake]
→ Captures current readings for scheduled sampling.


Level 3 – Historical Backfill

[OPCUAHistoryRead] → [PutSnowflake]
→ Retrieves long-term historical data for analytics.


Level 4 – Live Stream

[OPCUASubscribe] → [Snowflake Sink / Streamlit Dashboard]
→ Publishes real-time data changes continuously.


Level 5 – Command Writes

[GenerateCommand] → [OPCUAWriteData]
→ Sends setpoints or control signals to devices.

🧰 Dependencies (common across suite)
asyncua>=1.0.0,<2.0.0
cryptography>=41,<43
pandas>=2,<3
pyarrow>=14,<19
snowflake-connector-python>=3,<5

🏗️ Build & Deploy
Hatch / hatch-datavolo-nar Configuration
[tool.hatch.build.targets.nar]
sources = ["src"]
packages = ["processors"]

entry_points = [
  "net.ksmcloud.opcua.processors.OPCUAGetNodeData = processors.opcua_get_node_data:OPCUAGetNodeData",
  "net.ksmcloud.opcua.processors.OPCUAGetData = processors.opcua_get_data:OPCUAGetData",
  "net.ksmcloud.opcua.processors.OPCUAHistoryRead = processors.opcua_history_read:OPCUAHistoryRead",
  "net.ksmcloud.opcua.processors.OPCUASubscribe = processors.opcua_subscribe:OPCUASubscribe",
  "net.ksmcloud.opcua.processors.OPCUAWriteData = processors.opcua_write_data:OPCUAWriteData"
]

Directory Layout
src/
 └── processors/
      ├── opcua_get_node_data.py
      ├── opcua_get_data.py
      ├── opcua_history_read.py
      ├── opcua_subscribe.py
      └── opcua_write_data.py

Build Command
hatch build -t nar

Deploy

Copy the generated NAR(s) into:

/opt/runtime/extensions/


Restart your OpenFlow / NiFi runtime.
All processors will appear under the “Draconis Integrations — OPC UA” category.

🧭 Example Canvas Usage

Add Processor → OPCUAGetNodeData
Discover namespace & export metadata.

Add Processor → OPCUAGetData
Read current tag values for specific NodeIds.

Add Processor → OPCUASubscribe
Subscribe to live values; route results to Snowflake or file.

Add Processor → OPCUAHistoryRead
Retrieve historical tag data by time range.

Add Processor → OPCUAWriteData
Push control values back to connected OPC UA servers.

🧱 Example UNS Snowflake Schema
Layer	Table	Description
raw	OPCUA_EVENTS_RAW	Direct event streams from OPCUASubscribe.
bronze	OPCUA_TAGS	Flattened tag/value tables from GetData.
silver	OPCUA_HISTORY	Time-series history from HistoryRead.
gold	UNS_MODEL	Hierarchical node metadata from GetNodeData.
🧰 Troubleshooting
Issue	Likely Cause / Fix
BadServerUriInvalid	Switch Server URI Mode to encoded or raw.
Processor not visible in UI	Ensure __all__ and FlowFileSource definitions exist.
No data received	Verify NodeIds and endpoint URL; check port/firewall.
Snowflake errors	Confirm credentials and warehouse/database/schema/table exist.
UI lag	Reduce Browse Depth or subscription Buffer Events.
Large data bursts	Use CSV or Snowflake sink instead of JSON for efficiency.
🧾 License

MIT License ©
Free for commercial and industrial use.