
# SAP Hana Sink

## Description
This is a batch connector plugin for writing structured records to an SAP HANA database.

SAP HANA is a commercial in-memory database, built to accelerate data-driven decisions and 
real-time actions.

## Configuration
**Reference Name**: Name used to uniquely identify this sink for lineage, annotating metadata, etc.

### Endpoint Configuration
**Host**: The host of the database.

**Port**: The port of the database.

### Data Configuration
**Database Name**: The name of database. This field is optional.

**Table Name**: Name of the database table to export data to.

**Primary Key**: Name of the primary key of the database table to export data to.

### Authentication
**Username**: Name of a registered username. Required for databases that need authentication.
Optional otherwise.

**Password**: Password of the registered user. Required for databases that need authentication.
Optional otherwise.