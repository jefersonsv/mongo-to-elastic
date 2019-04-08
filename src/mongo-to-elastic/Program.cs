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
                .WriteTo.Console()
                .WriteTo.File(Path.Combine("logs", @"\log.txt"), rollingInterval: RollingInterval.Day)
                .CreateLogger();

            //Environment.SetEnvironmentVariable("TYPE", "sqlserver-to-mongo");
            //Environment.SetEnvironmentVariable("SQLSERVER", "Data Source=H73V220I;Initial Catalog=st1643_1; User Id=user_st1643_1;Password=pwd_st1643_1");

            SqlServerToMongo.Start().Wait();
            return;


            var crontab = NCrontab.CrontabSchedule.Parse("* * * * *");

            bool schedule = true;
            var dateTime = crontab.GetNextOccurrence(DateTime.Now);
            while (true)
            {
                if (schedule)
                {
                    var type = Environment.GetEnvironmentVariable("TYPE");

                    switch (type.ToLower().Trim())
                    {
                        case "monto-to-elastic":
                            JobManager.AddJob(() =>
                            {
                                MongoToElastic.Start().Wait();
                                dateTime = crontab.GetNextOccurrence(DateTime.Now);

                                schedule = true;
                            }, (s) => s.ToRunOnceAt(dateTime));
                            break;

                        case "sqlserver-to-mongo":
                            JobManager.AddJob(() =>
                            {
                                SqlServerToMongo.Start().Wait();
                                dateTime = crontab.GetNextOccurrence(DateTime.Now);

                                schedule = true;
                            }, (s) => s.ToRunOnceAt(dateTime));
                            break;

                        default:
                            throw new Exception("Type not recognized");
                    }

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