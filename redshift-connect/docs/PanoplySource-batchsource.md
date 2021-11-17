
# Panoply Source

## Description
This is a batch connector plugin to read data records from a Panoply data warehouse, and to transform 
them into structured data flow records.

Panoply is an Amazon Redshift based data warehouse, and acts due its various data connectors as a data
integration hub.

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

## Endpoint Configuration
**Host**: The host of the database.

**Port**: The port of the database.

## Data Configuration
**Database Name**: The name of the database.

**Table Name**: Name of the database table to import data from.

**Import Query**: The SQL select statement to import data from the database. For example:
select * from <your table name>.

## Authentication
**Username**: Name of a registered username. Required for databases that need authentication.
Optional otherwise.

**Password**: Password of the registered user. Required for databases that need authentication.
Optional otherwise.