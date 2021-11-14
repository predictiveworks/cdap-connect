
# Aerospike Sink

Description
---
This is a batch sink to write structured records to an Aerospike database. 

Aerospike is a NoSQL data platform to empower real-time, extreme scale data solutions 
that require high durability, predictable performance and operational simplicity.

A specialty of this batch sink is, that the *Aerospike key* is automatically derived
from the record field values. Each record field is mapped onto an Aerospike bin.

Configuration
---
Reference Name: Name used to uniquely identify this sink for lineage, annotating metadata, etc.