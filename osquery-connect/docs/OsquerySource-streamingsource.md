
# Osquery Source

Description
---
This is a Cyber Defense specific streaming connector plugin, based on Apache Kafka, to read real-time
endpoint query events (directly) published by deployed Osquery agents, and to transform them into 
structured data flow records.

Osquery is an operating system instrumentation framework for Windows, OS X (macOS), Linux, and FreeBSD. 
The tools compute low-level operating system analytics and monitoring both performant and intuitive.

Osquery exposes an operating system as a high-performance relational database. This allows to write 
SQL queries to explore operating system data. With Osquery, SQL tables represent abstract concepts 
such as running processes, loaded kernel modules, open network connections, browser plugins, hardware 
events or file hashes.

Configuration
---
Reference Name: Name used to uniquely identify this source for lineage, annotating metadata, etc.