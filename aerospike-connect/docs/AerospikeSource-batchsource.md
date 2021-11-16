
# Aerospike Source

## Description
This is a batch connector plugin to read data records from Aerospike namespaces and sets,
and to transform them into structured data flow records.

Aerospike is a commercial NoSQL data platform to empower real-time, extreme scale data solutions 
that require high durability, predictable performance and operational simplicity.

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

### Endpoint Configuration
**Host**: The host of the database.

**Port**: The port of the database.

**Timeout**: The timeout of a database connection in milliseconds. Default is 1000.

### Data Configuration:
**Namespace**: The name of the Aerospike namespace used to organize data.

**Set Name**: The name of the Aerospike set used to organize data.

### Authentication
**Username**: Name of a registered username. Required for authentication.

**Password**: Password of the registered user. Required for authentication.