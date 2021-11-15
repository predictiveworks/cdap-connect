
# Redshift Sink

Description
---
This is a batch connector plugin for writing structured records to an Amazon Redshift data warehouse. 
It is not recommended using this sink connector for (very) large datasets. In this case use the S3 connector, 
and the S3-to-Redshift action.

Amazon Redshift is a fully managed, petabyte-scale data warehouse service in the cloud. Regardless of
the data volume, Amazon Redshift offers fast query performance using SQL-based tools that ease the
integration with business intelligence applications.

Configuration
---

Reference Name: Name used to uniquely identify this sink for lineage, annotating metadata, etc.