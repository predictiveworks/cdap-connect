
# Ignite Source

## Description
This is a batch connector plugin to read data records from Apache Ignite in-memory caches, and to 
transform them into structured data flow records. In combination with the respective Ignite Sink 
plugin, in-memory data flows can be built for distributed high-speed ingestion, transformation and
advanced analytics.
 
Apache Ignite is a distributed database for high-performance computing with in-memory speed.

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

### Endpoint Configuration
**Host**: The host of the Apache Ignite cluster.

**Port**: The port of the Apache Ignite Cluster.

### Data Configuration
**Cache Name**: The name of the Apache Ignite cache used to organize data.

**Field Names**: The comma-separated list of field names that are used to extract from the specified cache.

**Partitions**: The number of partitions to organize the data of the specified cache. Default is 1.

### Authentication

**Username**: Optional. The name of the registered database user.

**Password**: Optional. The password of the registered database user

### SSL Security

**Use SSL**: Indicator to determine whether SSL transport security is used or not.

**Verify Certificates**: Optional: An indicator to determine whether certificates have to be verified. 
Supported values are 'true' and 'false'. If 'false', untrusted trust certificates (e.g. self-signed), 
will not lead to an error.

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

"configuration-groups": [
{
"label": "Data Configuration",
"properties": [
{
"widget-type": "textbox",
"label": "Field Names",
"name": "fieldNames"
},
{
"widget-type": "textbox",
"label": "Partitions",
"name": "partitions",
"widget-attributes": {
"default": "1"
}

]
