
# Crate Source

## Description
This is a batch connector plugin to read data records from a Crate database, and to transform them 
into structured data flow records.

CrateDB is a distributed SQL database built on top of a NoSQL foundation. It combines the familiarity of SQL
with the scalability and data flexibility of NoSQL, enabling users to:

* use SQL to process any type of data, structured or unstructured
* perform SQL queries at realtime speed, even JOINs and aggregates

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

### Endpoint Configuration
**Host**: The host of the database.

**Port**: The port of the database.

### Data Configuration
**Table Name**: Name of the database table to import data from.

**Import Query**: The SQL select statement to import data from the database. For example: 
select * from <your table name>.

### Authentication
**Username**: Name of a registered username. Required for databases that need authentication. 
Optional otherwise.

**Password**: Password of the registered user. Required for databases that need authentication. 
Optional otherwise.