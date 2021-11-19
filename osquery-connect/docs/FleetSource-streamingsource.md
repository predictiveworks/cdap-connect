
# Fleet Source

## Description
This is a Cyber Defense specific streaming connector plugin, based on Works Stream, to read real-time
endpoint query events published by a Fleet (Osquery) endpoint management platform, and to transform them
into structured data flow records.

Fleet is an Osquery-based endpoint management platform that supports deploying, managing and scaling
endpoint fleets with 100,000+ devices.

PredictiveWorks supports multiple approaches to connect to Osquery-based endpoints (query) events.
These events define a first-class starting point to monitor endpoints of an enterprise network and 
apply advanced analytics to e.g. detect anomalous behavior.

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

### Data Configuration
**Fleet Folder**: The file system folder used by the Fleet platform to persist log files.

**Log Extension**: Optional. The log file extension used by the Fleet platform. Default value is 'log'.

**Buffer Size**: Optional. The buffer size used to cache Fleet log entries. Default value is '1000'.

**Line Size**: Optional. The maximum number of bytes of a Fleet log entry. Default value is '8192'.

**Threads**: Optional. The number of threads used to process Fleet log entries. Default value is '1'.

**Polling Interval**: Optional. The polling interval in seconds to retrieve Fleet log entries.
Default value is '1'.
