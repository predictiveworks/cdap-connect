
# Kafka Source

## Description
This is a common purpose streaming connector plugin to read real-time events from Apache Kafka, 
and to transform them into structured data flow records.

Apache Kafka is an open-source distributed event streaming platform for high-performance data flows, 
streaming analytics, data integration, and mission-critical applications.

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

### Broker Configuration
**Brokers**: Specifies the list of Kafka brokers in host1:port1,host2:port2 form to determine the leader
for each topic. For example: host1.example.com:9092,host2.example.com:9092

**ConsumerProperties**: Specifies additional Kafka consumer properties to set in form of a key-value list.
For example: key1:value1, key2:value2 etc.

### Data Configuration
**Topic**: The Kafka topic from which messages are read.

**Time Field**: An optional name of the field containing the read time of the Kafka event batch. If this
is not set, a time field names '_timestamp' will be added to output records. If set, this field must be
present in the schema property and must be a long.

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
