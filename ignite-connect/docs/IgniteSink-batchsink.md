
# Ignite Sink

## Description
Apache Ignite is a distributed database for high-performance computing with in-memory speed.

## Configuration
**Reference Name**: Name used to uniquely identify this sink for lineage, annotating metadata, etc.

### Endpoint Configuration
**Host**: The host of the Apache Ignite cluster.

**Port**: The port of the Apache Ignite Cluster.

### Data Configuration
**Cache Name**: The name of the Apache Ignite cache used to organize data.

**Cache Mode**: Optional. The cache mode used when the provided cache does not exist. 
Supported values are 'partitioned' and 'replicated'. Default is 'partitioned'.

### Authentication

**Username**: Optional. The name of the registered database user.

**Password**: Optional. The password of the registered database user

### SSL Security

**Use SSL**: Indicator to determine whether SSL transport security is used or not.

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
