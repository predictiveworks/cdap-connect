
# Osquery Source

## Description
This is a Cyber Defense specific streaming connector plugin, based on Apache Kafka, to read real-time
endpoint query events (directly) published by deployed Osquery agents, and to transform them into 
structured data flow records.

Osquery is an operating system instrumentation framework for Windows, OS X (macOS), Linux, and FreeBSD. 
The tools compute low-level operating system analytics and monitoring both performant and intuitive.

Osquery exposes an operating system as a high-performance relational database. This allows to write 
SQL queries to explore operating system data. With Osquery, SQL tables represent abstract concepts 
such as running processes, loaded kernel modules, open network connections, browser plugins, hardware 
events or file hashes.

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

### Broker Configuration
**Brokers**: Specifies the list of Kafka brokers in host1:port1,host2:port2 form to determine the leader
for each topic. For example: host1.example.com:9092,host2.example.com:9092

**ConsumerProperties**: Specifies additional Kafka consumer properties to set in form of a key-value list.
For example: key1:value1, key2:value2 etc.

### Data Configuration
**Topic**: The Kafka topic from which messages are read.

**Topic Partitions**: The topic partitions to read from. If not specified, all partitions will be read.

**Initial Offset**: The default initial offset for all topic partitions. An offset of -2 means the smallest
offset. An offset of -1 means the latest offset. Defaults to -1. Offsets are inclusive. If an offset of 5 is
used, the message at offset 5 will be read. If you wish to set different initial offsets for different
partitions, use the 'Partition Offsets' property.

**Partition Offsets**: The initial offset for each topic partition. If this is not specified, all partitions
will have the same initial offset, which is determined by the 'Initial Offset' property. An offset of -2 means
the smallest offset. An offset of -1 means the latest offset. Offsets are inclusive. If an offset of 5 is used,
the message at offset 5 will be read.

**Max Rate Per Partition**: Maximum number of records to read per second and per partition. 0 means there is no
limit. Defaults to 1000.

### Authentication
**Kerberos Principal**: The Kerberos principal used when Kerberos security is enabled for Kafka.

**Keytab Location**: The keytab location for the Kerberos principal when Kerberos security is enabled for
Kafka.
