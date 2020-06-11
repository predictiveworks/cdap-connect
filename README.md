# CDAP-Connect

Purpose-built data connectors to extend Google CDAP (Cloud Data Fusion). 

Google CDAP provides a wide range of out-of-the-box connectors for common databases and warehouses, cloud platforms and services, and popular streaming platforms.

**CDAP-Connect** complements these common purpose connectors by adding e.g. connectors to low latency datastores, streaming platforms with real-time events especially referring to Cyber defense and Internet of Things data sources and sinks.

**CDAP-Connect** is built with CDAP API v6.2.0 and its connectors are open-sourced by Dr. Krusche & Partner.

## Data Stores

### Aerospike

### Apache Ignite

### Crate DB

### Influx DB

### SAP Hana

## Data Streaming

**CDAP-Connect** supports events streaming with respect to Kafka, MQTT, PubSub and WebSocket sources and sinks.

### Kafka

CDAP-Connect leverages Apache Spark Streaming to connect to Kafka based event streams. 

#### Default

CDAP-Connect supports automatic schema inference for *default* Kafka topics. Default topics originate from any Kafka source. The Apache Spark SQL inference mechanism is used to transform JSON topics into structured records for further processing in CDAP pipeline stages.

**Use case**: Processing event streams originating from Kafka brokers.

#### Osquery

Osquery daemons (endpoint agents) can be configured to forward query results as Kafka topics. Osquery shops with a pre-defined JSON format. CDAP-Connect provides automatic transformation of Osquery results into structured records.

**Use case**: Endpoint monitoring & event processing based on scheduled osquery events.

#### ThingsBoard

ThingsBoard can be configured to forward telemetry events (sensor readings) as Kafka topics. CDAP-Connect supports automatic schema inference for *ThingsBoard* Kafka topics. The Apache Spark SQL inference mechanism is used to transform JSON topics into structured records for further processing in CDAP pipeline stages.

**Use case**: Event processing of telemetry data originating from ThingsBoard.

#### Zeek

Zeek (former Bro) network monitor forwards a wide variety of network as Kafka topics. Each Zeek event type (DNS, HTTP, SNMP etc.) comes with pre-defined JSON format. CDAP-Connect provides automatic transformation of Zeek events, specified by a wide variety of event types, into structured records.

**Use case***: Network monitoring & event processing based on Zeek network monitor events.

### MQTT

CDAP-Connect leverages Apache Spark Streaming to connect to MQTT v3.1.x & v5 based event streams. 

#### Default

CDAP-Connect supports automatic schema inference for *default* MQTT topics. Default topics originate from any MQTT v3.1.x and v5 source. The current implementation supports two different MQTT clients: Eclipse Paho and HiveMQ.

* Eclipse Paho is an MQTT v3.1.x client and works best with Eclipse Mosquitto.

* HiveMQ is an advanced MQTT v3.1.x and v5 client and works best with HiveMQ brokers.

**Use case**: Processing event streams originating from MQTT brokers.

#### TheThings Network (TNN)

TheThings Network provides a LoRAWAN network server that exposes itself as an MQTT v3.1.x broker. CDAP-Connect supports uplink messages (topics) from this LoRaWAN server and provides automatic transformation of uplink events into structured records.

**Use case**: Event processing of uplink events originating from TheThings Network.

### PubSub

CDAP-Connect leverages Apache Spark Streaming to connect to Google PubSub based event streams. 

#### Default

Default PubSub events originate from any Google PubSource source with any further information about the associated schema. CDAP-Connect transforms this events into structured records by leveraging a simple (raw) message format. It is up to subsequent pipeline stages to extract more meaningful information from a PubSub message.

**Use case**: Processing event streams originating from Google PubSub.

#### Kolide

Kolide (Osquery) fleet management can be configured to forward Osquery results to Google PubSub. Kolide works as an event or log aggregator for its associated fleet of osquery daemons, and provides an alternative approach to connect to endpoint events.

**Use case**: Endpoint monitoring & event processing based on scheduled osquery events.

### WebSocket

CDAP-Connect leverages Apache Spark Streaming to connect to WebSocket based event streams. Support is currently restricted to events originating from Eclipse Ditto. 

**Use case**: Event processing based on Internet-of-Things platforms that support Eclipse Ditto service (e.g. Bosch IoT Suite).

## Data Warehouses

### Panoply

### Redshift

### Snowflake

## Graph Databases

### OrientDB