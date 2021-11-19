
# OpenCTI Source

## Description
This is a Cyber Defense specific streaming connector plugin to read real-time threat intelligence events 
from an OpenCTI platform, and to transform them into structured data flow records.

OpenCTI is an open source and unified platform for all levels of Cyber Threat Intelligence. It is made
to store, organize, visualize and share knowledge about cyber threats in the STIXv2 format.

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

### Endpoint Configuration
**Server URL**: The URL of the OpenCTI server.

## Data Configuration:
**Threads**: The number of threads used to process OpenCTI. Default value is '1'.

## Authentication
**Token**: Optional. The access token to authenticate the OpenCTI user.

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
