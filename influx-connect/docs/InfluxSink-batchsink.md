
# InfluxDB Sink

Description
---
This is a batch sink to write structured data records to an InfluxDB time series database.

InfluxDB is a high-performance data store written specifically for time series data. It allows 
for high throughput ingest, compression and real-time querying. It can handle millions of data 
points per second. 

A specialty of this batch sink is, that structured data record is divided into numeric and other 
fields. Numeric field values are transformed into Doubles and persisted as a measurement field.
String field values are persisted as tags, while all other fields are ignored.

Configuration
---
Reference Name: Name used to uniquely identify this sink for lineage, annotating metadata, etc.
