
# Redshift Sink

## Description
This is a batch connector plugin for writing structured records to an Amazon Redshift data warehouse. 
It is not recommended using this sink connector for (very) large datasets. In this case use the S3 connector, 
and the S3-to-Redshift action.

Amazon Redshift is a fully managed, petabyte-scale data warehouse service in the cloud. Regardless of
the data volume, Amazon Redshift offers fast query performance using SQL-based tools that ease the
integration with business intelligence applications.

## Configuration
**Reference Name**: Name used to uniquely identify this sink for lineage, annotating metadata, etc.

### Endpoint Configuration
**Host**: The host of the database.

**Port**: The port of the database.

### Data Configuration
**Database Name**: Name of the database to import data from.

**Table Name**: Name of the database table to export data to.

**Distribution Style**: The Redshift Distribution Style to be used when creating a table. Can be one of 
EVEN, KEY or ALL (see Redshift docs). When using KEY, you must also set a distribution key.

**Distribution Key**: The name of a column in the table to use as the distribution key when creating 
a table.

**Sort Key**: A full Redshift Sort Key definition. Examples include: SORTKEY(my_sort_column), 
COMPOUND SORTKEY(sort_col_1, sort_col_2), INTERLEAVED SORTKEY(sort_col_1, sort_col_2).

**Primary Key**: Name of the primary key of the database table to export data to.

### Authentication
**Username**: Name of a registered username. Required for databases that need authentication.
Optional otherwise.

**Password**: Password of the registered user. Required for databases that need authentication.
Optional otherwise.