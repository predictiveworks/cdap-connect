
# OrientDB Sink

## Description
This is a batch connector plugin for writing structured records to an Orient graph database.
A structured record either describes a graph vertex or an edge. Use cases where records specify 
vertices and edges are currently not supported by this plugin.

Orient is a multi-model open source NoSQL DBMS that combines the power of information 
networks or graphs, and the flexibility of documents into one scalable, high-performance 
operational database.

## Configuration
**Reference Name**: Name used to uniquely identify this sink for lineage, annotating metadata, etc.

### Endpoint Configuration
**Host**: The host of the database.

**Port**: The port of the database.

### Data Configuration
**Database Name**: The name of the database.

**Vertex Type**: Optional. The name of the vertex type if the provided dataset describes vertices.

**Edge Type**: Optional. The name of the edge type if the provided dataset describes edges.

**Write Option**: The write options. Supported values are 'Append', 'Overwrite', 'Ignore' and 'ErrorIfExists'. 
Default is 'Append'.

### Authentication
**Username**: Name of a registered username. Required for database authentication.

**Password**: Password of the registered user. Required for database authentication.
