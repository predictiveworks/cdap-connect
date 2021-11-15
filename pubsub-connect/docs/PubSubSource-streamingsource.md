
# PubSub Source

Description
---
This is a common purpose streaming connector plugin to read real-time events from Google PubSub,
and to transform them into structured data flow records.

Google PubSub allows services to communicate asynchronously, with latencies on the order of 100 
milliseconds. It is used for streaming analytics and data integration pipelines to ingest and 
distribute data. 

It is equally effective as messaging-oriented middleware for service integration or as a queue 
to parallelize tasks. PubSub enables to create systems of event producers and consumers, called 
publishers and subscribers. 

Configuration
---
Reference Name: Name used to uniquely identify this source for lineage, annotating metadata, etc.