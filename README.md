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

Either leverage [Google CDAP's](https://cdap.io) built-in user interface or [Predictive Works](https://predictiveworks.eu) more advanced pipeline interface to *visually* connect and configure these plugins (from Kafka to Crate) and millions of device and sensor readings are made available for subsequent analytics.

>Remainder: Data integration (e.g. connecting Crate Database with readings from ThingsBoard IoT platform) is not an end in itself. It is an important **but first step** towards data analytics to learn what these device & sensor readings tell about a manufacturer's production line or any other technical infrastructure.

**CDAP Spark** is the gateway to the world of IoT analytics, and, it is just another set of Google CDAP plugins.

Suppose trends within sensor readings have to be detected (e.g. a constant rise in the temperature of a certain machine), then **Works TS**, a module of CDAP Spark for time series analysis, can be used to extract trends in time series data. 

**Works TS** ships with an STL decomposition plugin for CDAP data pipelines. STL is short for *Seasonal and Trend decomposition using Loess* and is a proven algorithm to decompose a time signal into its seasonality, trend and remainder components.
