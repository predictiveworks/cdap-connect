# ThingsBoard

[ThingsBoard](https://thingsboard.io) is an open-source IoT platform for device management, data collection, processing & visualization.

ThingsBoard ships with an [Apache Kafka Plugin](https://thingsboard.io/docs/reference/plugins/kafka) that lets you forward device specific real-time events to Apache Kafka, e.g. for data analysis purposes.

The source connector has been built to consume real-time events for complex event processing and data ingestion 
into an IoT-scale database.  

The sink connector has been been built to send analyzed (asset or domain-specific) data back to ThingsBoard, e.g. for visualization. The result is provided as timeseries data of a tenant-specific Thingsboard asset with a tenant-specific type. If the specified asset does not exist, it is created by this data connector. 

<img src="https://github.com/predictiveworks/cdap-connect/blob/master/images/thingsboard-sink.png" width="800" alt="ThingsBoard Sink">