# Apache Kafka Connectors

CDAP Connect supports multiple Kafka-based streaming sources. The [StreamingSource] plugins support the following flavors: 

* **Default.** This plugin connects to a Kafka cluster and infers the provided event schema. Inference leverages the Spark SQL mechanism for JSON schema inference. 

* **Osquery.** Osquery endpoint agents publish query results as Kafka topics. In this use case, the event schema is predefined and the respective Osquery connector automatically transforms Kafka events into associated structured records.

* **ThingsBoard.** ThingsBoard is an open-source IoT platforms and supports Kafka forwarding of telemetry events. In this use case, the event schema is predefined and the respective ThingsBoard connector automatically transforms Kafka events into associated structured records. 

* **Zeek.** The Zeek (formerly Bro) network monitor publishes a variety of different network events as Kafka topic. In this use case, the event schema is predefined for each event type. The Zeek connector automatically transforms Kafka events into associated structured records.

<img src="https://github.com/predictiveworks/cdap-connect/blob/master/images/kafka-connect.png" width="800" alt="Kafka Connect">
