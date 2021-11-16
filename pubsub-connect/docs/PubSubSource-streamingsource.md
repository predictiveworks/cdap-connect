
# PubSub Source

## Description
This is a common purpose streaming connector plugin to read real-time events from Google PubSub,
and to transform them into structured data flow records.

Google PubSub allows services to communicate asynchronously, with latencies on the order of 100 
milliseconds. It is used for streaming analytics and data integration pipelines to ingest and 
distribute data. 

It is equally effective as messaging-oriented middleware for service integration or as a queue 
to parallelize tasks. PubSub enables to create systems of event producers and consumers, called 
publishers and subscribers. 

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

### Data Configuration
**Project ID**: Google Cloud Project ID, which uniquely identifies a project. It can be found on the 
Dashboard in the Google Cloud Platform Console.

**Subscription**: Cloud PubSub subscription to read from. If a subscription with the specified name 
does not exist, it will be automatically created, if a topic is specified. Messages published before 
the subscription was created will not be read.

**Topic**: Cloud PubSub topic to create a subscription on. This is only used when the specified 
subscription does not already exist and needs to be automatically created. If the specified subscription 
already exists, this value is ignored.

### Authentication
**Service Account File Path**: Path on the local file system of the service account key used for 
authorization. When running on clusters, the file must be present on every node in the cluster.
