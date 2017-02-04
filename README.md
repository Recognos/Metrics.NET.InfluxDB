# Metrics.NET.InfluxDB

InfluxDB reporter integration for Metrics.NET.

Current version: **v0.5.0-pre**

This library provides support for InfluxDB reporters using the [Metrics.NET](https://github.com/Recognos/Metrics.NET) monitoring and reporting library.

Supports InfluxDB Line Protocol for InfluxDB v0.9.1 and above.
Supports InfluxDB JSON Protocol for InfluxDB v0.9.1 and below.

The JSON protocol, which was previously used by the InfluxDB reporter, has been [deprecated](https://docs.influxdata.com/influxdb/v0.13/write_protocols/json/) and removed in InfluxDB versions greater than v0.9.1. The expected way to write metric data to InfluxDB after this point is by using the [LineProtocol syntax](https://docs.influxdata.com/influxdb/v0.13/write_protocols/line/) defined in the InfluxDB docs.

This library adds support for the newer versions of InfluxDB which use the line protocol syntax and supports both the HTTP and UDP protocols.

More documentation is availale in the [wiki](https://github.com/Recognos/Metrics.NET.InfluxDB/wiki).

[Changelog](https://github.com/Recognos/Metrics.NET.InfluxDB/blob/master/CHANGELOG.md)

[Metrics.NET Repository](https://github.com/Recognos/Metrics.NET)

## Configuration

Configuration can be done using the different overloads of the `MetricsReports` configuration object. For example, the following code will add an InfluxDB reporter using the HTTP protocol for a database named `testdb` on host `10.0.10.24:80`:
```
Metric.Config
	.WithReporting(report => report
		.WithInfluxDbHttp("10.0.10.24", "testdb", reportInterval, null, c => c
			.WithConverter(new DefaultConverter().WithGlobalTags("host=web1,env=dev"))
			.WithFormatter(new DefaultFormatter().WithLowercase(true))
			.WithWriter(new InfluxdbHttpWriter(c, 1000))));
```

## Extensibility

The InfluxDB report has been refactored to separate the writing process into a separate `InfluxdbWriter` which is responsible for writing the data using the whichever protocol is chosen. This also allows extending the model to support other or future types of protocols and formats defined by InfluxDB.

Writing data to InfluxDB is done in 3 different and extendable steps:
- `InfluxdbConverter`: Converts the metric data into `InfluxRecords`, which are the data model object for InfluxDB datapoints.
- `InfluxdbFormatter`: Formats the metric name and tag/field names used in the InfluxDB tables. For example, this can convert all names to lowercase or replace any spaces with underscores.
- `InfluxdbWriter`: Writes the `InfluxRecords` to the database. Derived implementations exist for the HTTP and UDP protocols.

## Building

_NOTE: All build scripts must be run from the repository root._

Run `build.bat` to compile and test the `Metrics.InfluxDB.dll` assmebly. Output gets copied to the `.\bin\` directory.

Run `create-nuget.bat` to build the solution and create a nuget `.nupkg` package in the `.\Publishing\` directory.

## License

This library will keep the same license as the main [Metrics.NET project](https://github.com/Recognos/Metrics.NET).

The main metrics project is released under the terms:
Copyright (c) 2016 The Recognos Metrics.NET Team
Published under Apache Software License 2.0, see [LICENSE](https://github.com/Recognos/Metrics.NET/blob/master/LICENSE)

This library (Metrics.NET.InfluxDB) is released under the Apache 2.0 License (see [LICENSE](https://github.com/Recognos/Metrics.NET.InfluxDB/blob/master/LICENSE)) 
Copyright (c) 2016 Jason O'Bryan, The Recognos Metrics.NET Team
