# (Industrial) Internet-of-Things Connectors

CDAP Connect supports 3 different strategies to interact and forward (I)IoT events:

* **First.** The "digital twin" pattern (see Eclipse Ditto plugin) supports users who want to abstract from and unify access to a huge amount of physical devices. With respect to existing IoT technologies, Eclipse Ditto is a new approach, but definitely future-oriented. 

* **Second.** MQTT is a lightweight & battle-tested IoT protocol. MQTT brokers are proven components in many (I)IoT networks to provide direct and indirect access to physical things. CDAP Connect supports MQTT 3.1.x and MQTT 5 brokers and users who have a need to subscribe and process device events and telemetry data without any abstraction.

* **Third.** CDAP Connect offers plugins to connect to (I)IoT platforms and supports users who have need to analyze pre-processed and forwarded telemetry data. The current implementations provides access to data forwarded by ThingsBoard.  


## Eclipse Ditto

**Eclipse Ditto** is an IoT technology that implements "digital twin" pattern. Such a "twin" is is a virtual, cloud based, representation of its physical or real world counterpart (charging stations, connected cars, sensors, smart heating, smart grids and more).

Eclipse Ditto is made to mirror potentially millions and billions of digital twins and thereby abstracts & unifies 
access and turns physical things into just another (web) service.

Eclipse Ditto is neither an IoT platform nor software that runs on an IoT gateway. It is designed as an intermediate layer to unify access to already connected devices, e.g. via **Eclipse Hono**). 

CDAP Connect uses Ditto's web socket API to subscribe to twin change events and live messages, and exposes them as Apache Spark real-time stream. The CDAP compliant [StreamingSource] plugin transforms events into structured records & publishes them to the subsequent pipeline stages.  

<img src="https://github.com/predictiveworks/cdap-connect/blob/master/images/ditto-connect.png" width="800" alt="Eclipse Ditto Connect">

**Eclipse Ditto** is currently restricted to MQTT 3.1.1.

## Eclipse Paho

**Eclipse Paho** is an open-source MQTT 3.1.1 client. CDAP-Connect uses this MQTT client to subscribe to a list of topics published by (but not restricted to) an MQTT broker of the Eclipse IoT Stack (i.e. Mosquitto). Respective events are exposed as an Apache Spark real-time stream.

Eclipse Paho is CDAP-Connect's default access to MQTT brokers. The schema of incoming events is automatically inferred and the events are transferred into structured records and published to subsequent pipeline stages.

The CDAP [StreamingSource], based on Eclipse Paho, also supports uplink messages from **The Things Network**. As the respective network server supports the MQTT protocol, this connector plugin can be used to directly connect to LoRaWAN devices.

<img src="https://github.com/predictiveworks/cdap-connect/blob/master/images/paho-connect.png" width="800" alt="Eclipse Paho Connect">


## HiveMQ

## ThingsBoard
