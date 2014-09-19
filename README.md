InfluxdbRpmAnnotationsCollector
===============================
Uses rpm -qa and additional rpm -qi <package_name> to abstract name,version,release. Pushes three different title types 'NEW,REMOVED,CHANGED'.
Used for rpm annotations querying on influxdb/grafana. Does not used inherited publish command from diamond.collector.Collector

Sends published data via influxdb

Must have read/write access to specified save_file

### Dependencies
	python-influxdb,
	rpm installed on local system
