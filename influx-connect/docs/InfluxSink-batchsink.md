
# InfluxDB Sink

## Description
This is a batch sink to write structured data records to an InfluxDB time series database.

InfluxDB is a high-performance data store written specifically for time series data. It allows 
for high throughput ingest, compression and real-time querying. It can handle millions of data 
points per second. 

A specialty of this batch sink is, that structured data record is divided into numeric and other 
fields. Numeric field values are transformed into Doubles and persisted as a measurement field.
String field values are persisted as tags, while all other fields are ignored.

## Configuration
**Reference Name**: Name used to uniquely identify this sink for lineage, annotating metadata, etc.

### Endpoint Configuration
**Host**: The host of the database.

**Port**: The port of the database.

**Protocol**: The protocol of the database connection. Supports values are 'http' and 'https'.

### Data Configuration
**Database**: The name of an existing InfluxDB database, used to write time points to.

**Measurement**: The name of the measurements used to write time points to.

**Time Field**: The optional name of the field in the input schema that contains the timestamp 
in milliseconds.

**Retention Interval**: The retention interval used when the database does not exist. Default is '30d'.

**Replication**: The replication factor used when the database does not exist. Default is 1.

### Authentication
**Username**: Name of a registered username. Required for authentication.

**Password**: Password of the registered user. Required for authentication.

