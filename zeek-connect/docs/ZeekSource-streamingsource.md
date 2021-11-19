
# Zeek Source

## Description
This is a Cyber Defense specific streaming connector plugin, based on Works Stream, to read real-time
events published a Zeek network traffic sensor, and to transform them into structured data flow records.

Zeek is an open source network traffic monitoring platform. Zeek acts as sensor and quietly and unobtrusively 
observes network traffic. It interprets what it sees and creates compact, high-fidelity transaction logs, file 
content, and fully customized output for deep threat analytics.

## Configuration
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

### Data Configuration
**Zeek Folder**: The file system folder used by the Zeek sensor to persist log files.

**Log Extension**: Optional. The log file extension used by the Zeek sensor. Default value is 'log'.

**Buffer Size**: Optional. The buffer size used to cache Zeek log entries. Default value is '1000'.

**Line Size**: Optional. The maximum number of bytes of a Zeek log entry. Default value is '8192'.

**Threads**: Optional. The number of threads used to process Zeek log entries. Default value is '1'.

**Polling Interval**: Optional. The polling interval in seconds to retrieve Zeek log entries. 
Default value is '1'.
