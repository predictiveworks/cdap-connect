# CDAP-Connect
Purpose-built data connectors for Google CDAP data pipelines

Google CDAP provides a wide range of out-of-the-box data connectors for data integration purposes. The list of connectors (far from complete) offer access to cloud solutions and platforms such as 

* Facebook
* HubSpot
* Marketo
* Salesforce
* SAP
* SendGrid
* Snowflake
* Splunk
* Zendesk
* Zuora

and more.

**CDAP-Connect** complements this list and offers data connectors that either improve ease-of-use of existing ones or appends connectors for business areas such as cyber defense and internet-of-things.

## Use Cases

### Internet-of-Things & Sensors

Stream millions of sensor readings per second into a [Crate](https://crate.io) database and query them in real-time, or, train deep learning & machine learning models, or more.

The image illustrates how readings from devices & sensors in an Internet-Of-Things environment can be streamed to a **Crate** database, and from there, connected to deep learning, machine learning or time series analysis.

<img src="https://github.com/predictiveworks/cdap-connect/blob/master/images/crate-sink.png" width="800" alt="Crate Sink">

This setting requires CDAP Connect's [Apache Kafka](https://kafka.apache.org) plugin to consume device and sensor readings from an IoT platform such as [ThingsBoard](https://thingsboard.io).

The Kafka plugin can be complemented by CDAP-Spark's query or rule plugin if intermediate event processing is necessary and finally sends the readings to CDAP Connect's [Crate](https://crate.io) plugin to persist this time series.

