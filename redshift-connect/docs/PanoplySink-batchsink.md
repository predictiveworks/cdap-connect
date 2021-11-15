
# Panoply Sink

Description
---
This is a batch connector plugin for writing structured records to a Panoply data warehouse. It is not 
recommended using this sink connector for (very) large datasets. In this case use the S3 connector, and 
the S3-to-Redshift action.

Panoply is an Amazon Redshift based data warehouse, and acts due its various data connectors as a data
integration hub.

Configuration
---
Reference Name: Name used to uniquely identify this sink for lineage, annotating metadata, etc.