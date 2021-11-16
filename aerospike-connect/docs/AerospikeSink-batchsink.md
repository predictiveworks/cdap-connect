
# Aerospike Sink

## Description
This is a batch connector plugin for writing structured records to an Aerospike database. 

Aerospike is a commercial NoSQL data platform to empower real-time, extreme scale data solutions 
that require high durability, predictable performance and operational simplicity.

A specialty of this batch sink is, that the *Aerospike key* is automatically derived
from the record field values. Each record field is mapped onto an Aerospike bin.

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

### Endpoint Configuration
**Host**: The host of the database.

**Port**: The port of the database.

**Timeout**: The timeout of a database connection in milliseconds. Default is 1000.

### Data Configuration:
**Namespace**: The name of the Aerospike namespace used to organize data.

**Set Name**: The name of the Aerospike set used to organize data.

**Expiration**: The expiration time of database records in milliseconds. Default is 0 (no expiration).

**Write Option**: The write options. Supported values are 'Append', 'Overwrite', 'Ignore' and 'ErrorIfExists'. 
Default is 'Append'.

### Authentication
**Username**: Name of a registered username. Required for authentication.

**Password**: Password of the registered user. Required for authentication.
