
# Kafka Stream Source

Description
-----------
This data connector is used when you want to read from an Apache Kafka real-time event stream. **Kafka Stream Source** expects events in **JSON** format
and automatically infers the associated data schema. 

Configuration
-----------

**Reference Name:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**Kafka Brokers:** List of Kafka brokers specified in host1:port1,host2:port2 form.

**Kafka Topic:** The Kafka topic to read from.

**Topic Partitions:** List of topic partitions to read from. If not specified, all partitions will be read.

**Default Initial Offset:** The default initial offset for all topic partitions. An offset of -2 means the smallest offset. An offset of -1 means the latest offset. Defaults to -1. Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read.

If you wish to set different initial offsets for different partitions, use the *Initial Partition Offsets* property.

**Initial Partition Offsets:** The initial offset for each topic partition. If this is not specified, all partitions will use the same initial offset, which is determined by the *Default Initial Offset* property. Any partitions specified in the partitions property, but not in this property will use the *Default Initial Offset*.

An offset of -2 means the smallest offset. An offset of -1 means the latest offset. Offsets are inclusive. If an offset of 5 is used, the message at offset 5 will be read.

**Format:** Optional format of the Kafka event message. Defaults to 'generic' which instructs this data connector to use the inferred data schema "AS IS".

**Time Field:** Optional name of the field containing the read time of the streaming batch. The time field will be added to the output records. If this is not set, an internal name '_timestamp' will be used.

**Max. Rate Per Partition:** Maximum number of records to read per second per partition. Defaults to 1000.

**Principal** The kerberos principal used for the source when kerberos security is enabled for kafka.

**Keytab Location** The keytab location for the kerberos principal when kerberos security is enabled for kafka.
