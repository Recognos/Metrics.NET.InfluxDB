using System;

namespace Metrics.InfluxDB.SamplesConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            Metric.Config
                .WithReporting(config => config
                    .WithInfluxDbHttp(new Uri(""), TimeSpan.FromSeconds(10)));
        }
    }
}
