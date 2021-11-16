
# Fleet PubSub Source

## Description
This is a Cyber Defense specific streaming connector plugin, based on Google PubSub, to read real-time
endpoint query events published by a Fleet (Osquery) endpoint management platform, and to transform them 
into structured data flow records.

Fleet is an Osquery-based endpoint management platform that supports deploying, managing and scaling
endpoint fleets with 100,000+ devices.

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
