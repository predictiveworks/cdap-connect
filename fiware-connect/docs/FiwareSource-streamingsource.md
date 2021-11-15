
# Fiware Source

Description
---
This is an IoT specific streaming connector plugin to read real-time events from a Fiware notification
server, that refer to configured subscriptions. Subscriptions are sent to a Context Broker, and the
broker sends IoT events to the notification server that can be consumed for further processing.

This plugin connects to Works Stream that manages and operates a Fiware notification server and publishes
incoming events as real-time stream.

Fiware is an open source initiative to facilitate and accelerate the development of smart IoT solutions. 
Main component is the Fiware Context Broker that acts as a common data hub and middleware. One of the
major benefits of this approach is, that notifications are published in a standardized NGSIv2 format. 

Configuration
---
Reference Name: Name used to uniquely identify this source for lineage, annotating metadata, etc.
