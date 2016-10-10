using System;
using Metrics.InfluxDB.Adapters;
using Metrics.InfluxDB.Model;

namespace Metrics.InfluxDB
{
	/// <summary>
	/// A metrics report that sends data to an InfluxDB server. This sends the data using the InfluxDB JSON protocol over HTTP.
	/// NOTE: It is recommended to NOT use the JSON reporter because of performance issues, and support for JSON has been
	/// removed from InfluxDB in versions later than v0.9.1. It is recommended to use the HTTP or UDP reporters instead.
	/// See for more information: https://docs.influxdata.com/influxdb/v0.13/write_protocols/json/
	/// </summary>
	public class InfluxdbJsonReport : InfluxdbBaseReport
	{
		/// <summary>
		/// Creates a new InfluxDB report that uses the JSON protocol over HTTP.
		/// NOTE: It is recommended to NOT use the JSON reporter because of performance issues, and support for JSON has been
		/// removed from InfluxDB in versions later than v0.9.1. It is recommended to use the HTTP or UDP reporters instead.
		/// See for more information: https://docs.influxdata.com/influxdb/v0.13/write_protocols/json/
		/// </summary>
		/// <param name="influxDbUri">The URI of the InfluxDB server, including any query string parameters.</param>
		public InfluxdbJsonReport(Uri influxDbUri)
			: base(influxDbUri) {
		}

		/// <summary>
		/// Creates a new InfluxDB report that uses the JSON protocol over HTTP.
		/// NOTE: It is recommended to NOT use the JSON reporter because of performance issues, and support for JSON has been
		/// removed from InfluxDB in versions later than v0.9.1. It is recommended to use the HTTP or UDP reporters instead.
		/// See for more information: https://docs.influxdata.com/influxdb/v0.13/write_protocols/json/
		/// </summary>
		/// <param name="config">The InfluxDB configuration object.</param>
		public InfluxdbJsonReport(InfluxConfig config = null)
			: base(config) {
		}

		/// <summary>
		/// Gets the default configuration by setting the <see cref="InfluxConfig.Writer"/>
		/// property to a new <see cref="InfluxdbJsonWriter"/> instance if it is not set.
		/// The <see cref="InfluxConfig.Formatter"/> is set to a no-op default formatter that does not modify identifier names, if the formatter is not set.
		/// The <see cref="InfluxConfig.Precision"/> is set to <see cref="InfluxPrecision.Seconds"/> because it is the only precision supported by the JSON protocol.
		/// </summary>
		/// <param name="defaultConfig">The configuration to apply the settings to. If null, creates a new <see cref="InfluxConfig"/> instance.</param>
		/// <returns>A default <see cref="InfluxConfig"/> instance for the derived type's implementation.</returns>
		protected override InfluxConfig GetDefaultConfig(InfluxConfig defaultConfig) {
			var config = base.GetDefaultConfig(defaultConfig) ?? new InfluxConfig();
			config.Precision = InfluxPrecision.Seconds; // JSON only supports seconds
			config.Formatter = config.Formatter ?? GetDefaultFormatter();
			config.Writer = config.Writer ?? new InfluxdbJsonWriter(config);
			return config;

		}

		private InfluxdbFormatter GetDefaultFormatter() {
			var formatter = new DefaultFormatter(false, null);
			formatter.ContextNameFormatter = null;
			formatter.MetricNameFormatter  = null;
			formatter.TagKeyFormatter      = null;
			formatter.FieldKeyFormatter    = null;
			return formatter;
		}
	}
}
