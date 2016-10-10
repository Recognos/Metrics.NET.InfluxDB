using System;
using Metrics.InfluxDB.Model;
using Metrics.InfluxDB.Adapters;

namespace Metrics.InfluxDB
{
	/// <summary>
	/// A metrics report that sends data to an InfluxDB server.
	/// This sends the data using the InfluxDB LineProtocol syntax over UDP.
	/// </summary>
	public class InfluxdbUdpReport : DefaultInfluxdbReport
	{
		/// <summary>
		/// Creates a new InfluxDB report that uses the Line Protocol syntax over UDP.
		/// </summary>
		/// <param name="influxDbUri">The UDP URI of the InfluxDB server.</param>
		public InfluxdbUdpReport(Uri influxDbUri)
			: base(influxDbUri) {
		}

		/// <summary>
		/// Creates a new InfluxDB report that uses the Line Protocol syntax over UDP.
		/// </summary>
		/// <param name="config">The InfluxDB configuration object.</param>
		public InfluxdbUdpReport(InfluxConfig config = null)
			: base(config) {
		}

		/// <summary>
		/// Gets the default configuration by setting the <see cref="InfluxConfig.Writer"/>
		/// property to a new <see cref="InfluxdbUdpWriter"/> instance if it is not set.
		/// </summary>
		/// <param name="defaultConfig">The configuration to apply the settings to. If null, creates a new <see cref="InfluxConfig"/> instance.</param>
		/// <returns>A default <see cref="InfluxConfig"/> instance for the derived type's implementation.</returns>
		protected override InfluxConfig GetDefaultConfig(InfluxConfig defaultConfig) {
			var config = base.GetDefaultConfig(defaultConfig) ?? new InfluxConfig();
			config.Writer = config.Writer ?? new InfluxdbUdpWriter(config);
			return config;
		}
	}
}
