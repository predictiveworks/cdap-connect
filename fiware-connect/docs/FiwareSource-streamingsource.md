
# Fiware Source

## Description
This is an IoT specific streaming connector plugin to read real-time events from a Fiware notification
server, that refer to configured subscriptions. Subscriptions are sent to a Context Broker, and the
broker sends IoT events to the notification server that can be consumed for further processing.

This plugin connects to Works Stream that manages and operates a Fiware notification server and publishes
incoming events as real-time stream.

Fiware is an open source initiative to facilitate and accelerate the development of smart IoT solutions. 
Main component is the Fiware Context Broker that acts as a common data hub and middleware. One of the
major benefits of this approach is, that notifications are published in a standardized NGSIv2 format. 

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

### Endpoint Configuration
**Server Host**: The host address of the Fiware notification endpoint.

**ServerPort**: The port of the Fiware notification endpoint.

**Broker URL**: The url of the Fiware broker.

### Data Configuration
**Subscriptions**: The Fiware Broker subscription(s).

**Threads**: The number of threads used to process Fiware notifications. Default value is '1'.

## SSL Server Configuration

**Cipher Suites**: Optional. A comma-separated list of cipher suites which are allowed for a secure connection.
Samples are TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, TLS_RSA_WITH_AES_128_GCM_SHA256 and others.

**Keystore Path**: Optional. A path to a file which contains the client SSL keystore for the Fiware Server.

**Keystore Type**: Optional. The format of the client SSL keystore for the Fiware Server. Supported values are 'JKS', 'JCEKS'
and 'PKCS12'.

**Keystore Algo**: Optional. The algorithm used for the client SSL keystore for the Fiware Server.

**Keystore Pass**: Optional. The password of the client SSL keystore for the Fiware Server.

**Truststore Path**: Optional. A path to a file which contains the client SSL truststore for the Fiware Server.

**Truststore Type**: Optional. The format of the client SSL truststore for the Fiware Server. Supported values are 'JKS', 'JCEKS'
and 'PKCS12'.

**Truststore Algo**: Optional. The algorithm used for the client SSL truststore for the Fiware Server.

**Truststore Pass**: Optional. The password of the client SSL truststore for the Fiware Server.

## SSL Broker Configuration

**Cipher Suites**: Optional. A comma-separated list of cipher suites which are allowed for a secure connection.
Samples are TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, TLS_RSA_WITH_AES_128_GCM_SHA256 and others.

**Keystore Path**: Optional. A path to a file which contains the client SSL keystore for the Fiware Broker.

**Keystore Type**: Optional. The format of the client SSL keystore for the Fiware Broker. Supported values are 'JKS', 'JCEKS'
and 'PKCS12'.

**Keystore Algo**: Optional. The algorithm used for the client SSL keystore for the Fiware Broker.

**Keystore Pass**: Optional. The password of the client SSL keystore for the Fiware Broker.

**Truststore Path**: Optional. A path to a file which contains the client SSL truststore for the Fiware Broker.

**Truststore Type**: Optional. The format of the client SSL truststore for the Fiware Broker. Supported values are 'JKS', 'JCEKS'
and 'PKCS12'.

**Truststore Algo**: Optional. The algorithm used for the client SSL truststore for the Fiware Broker.

**Truststore Pass**: Optional. The password of the client SSL truststore for the Fiware Broker.
