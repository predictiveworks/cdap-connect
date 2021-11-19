
# Paho Source

## Description
This is an IoT specific streaming connector plugin to read real-time events from an MQTTv3.1 broker,
and to transform them into structured data flow records.

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

### Endpoint Configuration
**Broker Address**: The address of the MQTT broker to connect to, including protocol and port.

### Data Configuration:

**Topics**: The comma-separated list of MQTT topics to listen to.

**Client ID**: Optional. The MQTT client identifier.

**QoS**: Optional. The MQTT quality of service specification. Default is 'at-most-once.

**Version**: Optional. The version of the MQTT protocol. Default is 'mqtt-v31.

### Authentication

**Username**: Optional. The name of the registered MQTT user.

**Password**: Optional. The password of the registered MQTT user

### SSL Security

**Cipher Suites**: Optional. A comma-separated list of cipher suites which are allowed for a secure connection. 
Samples are TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, TLS_RSA_WITH_AES_128_GCM_SHA256 and others.

**Keystore Path**: Optional. A path to a file which contains the client SSL keystore.

**Keystore Type**: Optional. The format of the client SSL keystore. Supported values are 'JKS', 'JCEKS' 
and 'PKCS12'.

**Keystore Algo**: Optional. The algorithm used for the client SSL keystore.

**Keystore Pass**: Optional. The password of the client SSL keystore.

**Truststore Path**: Optional. A path to a file which contains the client SSL truststore.

**Truststore Type**: Optional. The format of the client SSL truststore. Supported values are 'JKS', 'JCEKS' 
and 'PKCS12'.

**Truststore Algo**: Optional. The algorithm used for the client SSL truststore.

**Truststore Pass**: Optional. The password of the client SSL truststore.
