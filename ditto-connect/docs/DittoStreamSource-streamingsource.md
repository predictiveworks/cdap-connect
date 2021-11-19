
# Eclipse Ditto Source

## Description
This is an IOT specific streaming connector plugin to read Web socket real-time events
from Eclipse Ditto, and to transform them into structured data flow records. This plugin 
is part of PredictiveWorks. IoT support and contributes to its semantic abstraction approach.

Eclipse Ditto abstracts an IoT device into a digital twin and provides synchronous and 
asynchronous APIs to work with physical devices. Ditto is an IoT middleware to interact 
with real world assets via the digital twin pattern

Devices are integrated via device connectivity layers like Eclipse Hono or e.g. MQTT 
brokers like Eclipse Mosquitto. Digital twins managed by Ditto may either forward their 
changes or receive live commands to actuate.

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

## Endpoint Configuration
**Endpoint**: The endpoint of the Ditto (Thing) service endpoint.

## Thing & Features Identifier

**Thing ID**: The unique identifier of a certain thing. if provided, thing specific subscriptions are 
restricted to this thing.

**Feature ID**: The unique identifier of a certain feature. if provided, feature specific subscriptions 
are restricted to this feature.

## Change Events 

**Thing Changes**: An indicator to determine whether to listed to changes of all things. 
Supported values are 'true' and 'false'. Default is 'true'.

**Features Changes**: An indicator to determine whether to listed to all feature set changes of all things. 
Supported values are 'true' and 'false'. Default is 'false'.

**Feature Changes**: An indicator to determine whether to listed to all feature changes of all things. 
Supported values are 'true' and 'false'. Default is 'false'.

**Live Messages**: An indicator to determine whether to listed to all message sent by of all things. 
Supported values are 'true' and 'false'. Default is 'false'

## Authentication

Eclipse Ditto supports basic authentication based on the name of the user and his password. 
Alternatively OAuth authentication is supported. 

**Username**: The name of the user registered with the Ditto (Thing) service. Required for basic
authentication only.

**Password**: The password  of the user registered with the Ditto (Thing) service. Required for 
basic authentication only.

**Client ID**: Client identifier obtained during the OAuth registration process. Required for OAuth
authentication only.

**Client Secret**: Client secret obtained during the OAuth registration process. Required for OAuth
authentication only.

**Token Endpoint**: Endpoint for the resource server, which exchanges the authorization code for 
an access token.

**Scope(s)**: Scope of the access request, which might have multiple comma-separated values.

**Proxy Host**: The proxy host optionally used by Eclipse Ditto's authentication provider.

**Proxy Port**: The proxy port optionally used by Eclipse Ditto's authentication provider.

**Truststore Location**: A path to a file which contains the truststore.

**Truststore Password**: Password for a truststore.
