using FluentScheduler;
using Serilog;
using System;
using System.IO;
using System.Threading.Tasks;

namespace mongo_to_elastic
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.ColoredConsole()
                .WriteTo.RollingFile(Path.Combine("logs", "log-{Date}.log"))
                .CreateLogger();

            var crontab = NCrontab.CrontabSchedule.Parse("* * * * *");

            bool schedule = true;
            var dateTime = crontab.GetNextOccurrence(DateTime.Now);
            while (true)
            {
                if (schedule)
                {
                    JobManager.AddJob(() =>
                    {
                        Sync.Start().Wait();
                        dateTime = crontab.GetNextOccurrence(DateTime.Now);

                        schedule = true;
                    }, (s) => s.ToRunOnceAt(dateTime));

                    Log.Information($"Scheduled to: {dateTime}");
                    schedule = false;
                }
                else
                {
                    Task.Delay(TimeSpan.FromMinutes(1).Seconds).Wait();
                }
            }
        }
    }
}