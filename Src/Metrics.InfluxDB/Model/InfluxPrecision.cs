namespace Metrics.InfluxDB.Model
{
	/// <summary>
	/// The precision to format timestamp values in the InfluxDB LineProtocol syntax.
	/// </summary>
	public enum InfluxPrecision
	{
		/// <summary>
		/// Nanosecond precision. This is the default precision used by InfluxDB when no precision specifier is defined.
		/// </summary>
		Nanoseconds = 0,

		/// <summary>
		/// Microsecond precision.
		/// </summary>
		Microseconds,

		/// <summary>
		/// Millisecond precision.
		/// </summary>
		Milliseconds,

		/// <summary>
		/// Second precision.
		/// </summary>
		Seconds,

		/// <summary>
		/// Minute precision.
		/// </summary>
		Minutes,

		/// <summary>
		/// Hour precision.
		/// </summary>
		Hours
	}
}
