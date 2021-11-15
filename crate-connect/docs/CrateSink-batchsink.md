
# Crate Sink

Description
---
This is a batch connector plugin for writing structured records to a Crate database.

Crate is a distributed SQL database built on top of a NoSQL foundation. It combines 
the familiarity of SQL with the scalability and data flexibility of NoSQL, enabling 
users to:

* use SQL to process any type of data, structured or unstructured
* perform SQL queries at realtime speed, even JOINs and aggregates

A specialty of this batch sink is, that it does not require previous stages to provide 
an input schema; if there is no schema available, the schema is inferred from the first
record, and a new schema-compliant table is created on the fly.

Configuration
---
Reference Name: Name used to uniquely identify this sink for lineage, annotating metadata, etc.
