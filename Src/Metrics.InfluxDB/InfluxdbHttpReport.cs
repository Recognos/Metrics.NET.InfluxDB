using System;
using Metrics.InfluxDB.Model;
using Metrics.InfluxDB.Adapters;

namespace Metrics.InfluxDB
{
	/// <summary>
	/// A metrics report that sends data to an InfluxDB server.
	/// This sends the data using the InfluxDB LineProtocol syntax over HTTP.
	/// </summary>
	public class InfluxdbHttpReport : DefaultInfluxdbReport
	{
		/// <summary>
		/// Creates a new InfluxDB report that uses the Line Protocol syntax over HTTP.
		/// </summary>
		/// <param name="influxDbUri">The URI of the InfluxDB server, including any query string parameters.</param>
		public InfluxdbHttpReport(Uri influxDbUri)
			: base(influxDbUri) {
		}

		/// <summary>
		/// Creates a new InfluxDB report that uses the Line Protocol syntax over HTTP.
		/// </summary>
		/// <param name="config">The InfluxDB configuration object.</param>
		public InfluxdbHttpReport(InfluxConfig config = null)
			: base(config) {
		}

		/// <summary>
		/// Gets the default configuration by setting the <see cref="InfluxConfig.Writer"/>
		/// property to a new <see cref="InfluxdbHttpWriter"/> instance if it is not set.
		/// </summary>
		/// <param name="defaultConfig">The configuration to apply the settings to. If null, creates a new <see cref="InfluxConfig"/> instance.</param>
		/// <returns>A default <see cref="InfluxConfig"/> instance for the derived type's implementation.</returns>
		protected override InfluxConfig GetDefaultConfig(InfluxConfig defaultConfig) {
			var config = base.GetDefaultConfig(defaultConfig) ?? new InfluxConfig();
			config.Writer = config.Writer ?? new InfluxdbHttpWriter(config);
			return config;
		}
	}
}
