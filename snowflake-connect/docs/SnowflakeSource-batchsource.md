
# Snowflake Source

## Description
This is a batch connector plugin to read data records from a Snowflake data warehouse, and to transform
them into structured data flow records.

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

## Endpoint Configuration
**Host**: The host of the database.

**Port**: The port of the database.

## Data Configuration
**Warehouse Name**: Name of the Snowflake data warehouse to import data from.

**Account**: Name of the Snowflake account.

**Database Name**: Name of the database to import data from.

**Database Schema**: Name of the default schema to use for the specified database
once connected, or an empty string. The specified schema should be an existing schema 
for which the specified default role has privileges.

**Table Name**: Name of the database table to import data from.

**Import Query**: The SQL select statement to import data from the database. For example:
select * from <your table name>.

## Authentication
**Username**: Name of a registered username. Required for databases that need authentication.
Optional otherwise.

**Password**: Password of the registered user. Required for databases that need authentication.
Optional otherwise.
