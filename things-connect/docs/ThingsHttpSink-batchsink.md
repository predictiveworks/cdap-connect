
# Things Http Sink

## Description
This is an IoT specific batch connector plugin for writing structured records to the IoT platform 
ThingsBoard via HTTP.

ThingsBoard is an open source IoT platform for device management, data collection, processing and 
visualization. Its HTTP API can be used for sending asset and device specific data to this platform.

## Configuration
**Reference Name**: Name used to uniquely identify this sink for lineage, annotating metadata, etc.

### Endpoint Configuration
**Host**: The host of the Thingsboard server.

**Port**: The port of the Thingsboard server.

### Data Configuration
**Asset Name**: The name of the asset.

**Asset Type**: The asset type.

**Asset Limit**: The number of assets of the specified asset type to take into account to decide 
whether an asset with the provided name already exists. Default is 1000.

**Asset Features**: A comma-separated list of field names, that describe the features of the sending 
asset.

### Authentication

**Username**: Optional. The name of the registered ThingsBoard user.

**Password**: Optional. The password of the registered ThingsBoard user
