🐉 OPCUASubscribe — Snowflake OpenFlow / NiFi Python Source Processor

Version: 0.7.0
Namespace: net.ksmcloud.opcua.processors.OPCUASubscribe
Author: Kevin Mahnke
License: MIT

🧩 Overview

OPCUASubscribe is a continuous OPC UA subscription processor for Snowflake OpenFlow / Apache NiFi, designed for reliable, low-latency streaming of live industrial data.

It subscribes to multiple OPC UA nodes, applies client- or server-side deadband filters, and emits changes in JSON, CSV, or XML — or directly writes to Snowflake in near-real-time.

Ideal for:

Live telemetry feeds from PLCs and DCS systems

Unified Namespace (UNS) streaming layers

Hybrid edge-to-cloud pipelines

Data-governed IIoT ingestion (ISA-95 / Industry 4.0-ready)

⚙️ Key Features
Capability	Description
🔄 Continuous Subscription	Maintains persistent OPC UA sessions with automatic reconnect and client-side buffering.
🧠 Smart Deadband Filtering	Supports client or server mode with Absolute / Percent thresholds.
🧾 Flexible Output Formats	Emit JSON, CSV, XML, or write directly into Snowflake tables.
❄️ Snowflake Sink Integration	Stream OPC UA changes straight to Snowflake — schema auto-creation optional.
🔌 Server URI Control	Auto-discover or override ApplicationUri with auto / raw / encoded / empty modes.
🧰 UI Discoverability	Always visible in the OpenFlow canvas, even without asyncua installed.
📊 Diagnostics & Heartbeats	Emits empty heartbeats during quiet periods to maintain flow continuity.
📦 Dependencies
asyncua>=1.0.0,<2.0.0
cryptography>=41,<43
pandas>=2,<3           # optional for Snowflake
pyarrow>=14,<19        # optional for Snowflake
snowflake-connector-python>=3,<5  # optional for Snowflake


✅ Only asyncua + cryptography are required for OPC UA communication.
🧊 Snowflake support requires pandas, pyarrow, and snowflake-connector-python.

🧠 Properties
Property	Description	Default
OPC UA Endpoint	Full endpoint URL (e.g. opc.tcp://host:port/Server).	(required)
Server URI Override	Optional explicit ApplicationUri. Auto-discovered if blank.	(empty)
Server URI Mode	`auto	raw
NodeIds (CSV)	Comma-separated list of NodeIds to subscribe to.	(required)
Sampling Interval (ms)	Requested server sampling period.	250
Monitored Item Queue Size	Server queue depth per item.	256
Filter Mode	client = edge filtering server = OPC UA filter	client
Trigger	Server-side trigger type (Status, Value, StatusValue, StatusValueTimestamp).	StatusValue
Deadband Absolute / Percent	Thresholds for client or server mode filtering.	(empty)
Block Until Event (ms)	Wait window before returning when idle.	5000
Emit Initial On Connect	Emit initial values upon (re)connect.	true
Max Client Buffer Events	In-memory buffer size for queued changes.	50000
Timeout (sec)	Client operation timeout.	10
Username / Password	Optional OPC UA credentials.	(empty)
Output Format	json, csv, xml, snowflake.	json
CSV Delimiter / Include Header	Customize CSV output.	, / true
XML Root / Row Element	Customize XML tag names.	opcuaEvents / event
SF Account / User / Password / Role / Warehouse / Database / Schema / Table	Snowflake connection properties.	(empty)
SF Auto-Create Table	Create table automatically if missing.	true
Emit Heartbeat on Empty	Send heartbeat payload on idle triggers.	true
Debug Level	`none	basic
🔀 Relationships
Relationship	Description
success	Subscription events or Snowflake write summary emitted.
empty	No new events — heartbeat or 0-byte payload emitted.
failure	Processing error (text payload).
🧾 Output Examples
JSON
{
  "mode": "subscribe",
  "endpoint": "opc.tcp://93.92.30.18:21381/MatrikonOpcUaWrapper",
  "timestamp_utc": "2025-10-21T06:00:00Z",
  "events": [
    {
      "nodeId": "ns=2;s=Compressor.Pressure",
      "value": 42.7,
      "status": "Good",
      "sourceTimestamp": "2025-10-21T06:00:00Z"
    }
  ]
}

CSV
nodeId,value,status,sourceTimestamp
ns=2;s=Compressor.Pressure,42.7,Good,2025-10-21T06:00:00Z

XML
<opcuaEvents endpoint="opc.tcp://93.92.30.18:21381/MatrikonOpcUaWrapper">
  <event>
    <nodeId>ns=2;s=Compressor.Pressure</nodeId>
    <value>42.7</value>
    <status>Good</status>
    <sourceTimestamp>2025-10-21T06:00:00Z</sourceTimestamp>
  </event>
</opcuaEvents>

Snowflake Sink Summary
{
  "mode": "subscribe",
  "endpoint": "opc.tcp://93.92.30.18:21381/MatrikonOpcUaWrapper",
  "timestamp_utc": "2025-10-21T06:00:00Z",
  "events_written": 128,
  "table": "OPCUA_DB.PUBLIC.OPCUA_EVENTS"
}

🧊 Snowflake Integration
Column	Type	Description
NODE_ID	STRING	OPC UA NodeId
VALUE	VARIANT	Tag value
STATUS	STRING	OPC UA StatusCode
SOURCE_TS	TIMESTAMP_TZ	Source timestamp
ENDPOINT	STRING	OPC UA server endpoint
INGEST_TS	TIMESTAMP_TZ	Ingestion timestamp

To enable:

Set Output Format → snowflake

Provide all SF Account/User/Password/WH/DB/Schema/Table fields

Optionally enable SF Auto-Create Table

🏗️ Build & Deploy
Hatch / hatch-datavolo-nar Configuration
[tool.hatch.build.targets.nar]
sources = ["src"]
packages = ["processors"]

entry_points = [
  "net.ksmcloud.opcua.processors.OPCUASubscribe = processors.opcua_subscribe:OPCUASubscribe"
]

Directory Layout
src/
 └── processors/
      └── opcua_subscribe.py

Build Command
hatch build -t nar

Deploy to OpenFlow

Copy the generated NAR file to:

/opt/runtime/extensions/


Then restart the OpenFlow runtime.

🧭 Canvas Usage Example

Add Processor → OPCUASubscribe

Configure Properties

OPC UA Endpoint:  opc.tcp://93.92.30.18:21381/MatrikonOpcUaWrapper
NodeIds (CSV):    ns=2;s=Compressor.Pressure,ns=2;s=Compressor.Temperature
Filter Mode:      client
Deadband Absolute: 0.2
Output Format:    json
Block Until Event (ms): 5000


Connect success → PutSnowflake or PutFile

Start the flow; verify events stream in real time.

🧱 Example UNS Architecture
[ OPCUAGetNodeList ] → [ OPCUAGetData ] → [ OPCUASubscribe ]
                                     ↓
                               [ Transform ]
                                     ↓
                               [ PutSnowflake ]


Forms the real-time bronze layer in a Snowflake Unified Namespace (UNS) pipeline.

🧰 Troubleshooting
Symptom	Likely Cause / Fix
BadServerUriInvalid	Try Server URI Mode = encoded or raw with explicit ApplicationUri.
No events received	Increase sampling interval or verify NodeIds exist.
Heartbeat only	Normal behavior when no changes detected.
Processor missing in UI	Ensure __all__=["OPCUASubscribe","create"] and FlowFileSource inheritance.
Snowflake write fails	Confirm network access and all SF connection fields.
High memory usage	Reduce Max Client Buffer Events.
🧾 License

MIT License © 2025
Free for industrial and commercial use.