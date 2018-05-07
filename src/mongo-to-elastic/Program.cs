using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading.Tasks;

namespace mongo_to_elastic
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Start(args).Wait();
            //Console.ReadLine();
            Console.WriteLine("Finish");
        }

        private static async Task Start(string[] args)
        {
            var mongodb = Environment.GetEnvironmentVariable("MONGODB");
            var elasticsearch = Environment.GetEnvironmentVariable("ELASTICSEARCH");

            // Nest
            var settings = new Nest.ConnectionSettings(new Uri(elasticsearch))
                .DefaultIndex("whole-cheap");

            var elastic = new Nest.ElasticClient(settings);
            elastic.DeleteIndex("whole-cheap");
            if (!elastic.IndexExists("whole-cheap").Exists)
            {
                elastic.CreateIndex("whole-cheap", c => c
                .Settings(s => s
                    .NumberOfShards(1)
                    .NumberOfReplicas(0)
                )
                .Mappings(m => m
                    .Map<BsonDocument>(d => d
                        .AutoMap()
                    )
                ));
            }

            var client = new MongoClient(mongodb);
            var database = client.GetDatabase("whole-cheap");
            var ctrl = database.GetCollection<BsonDocument>("__mongo-to-elastic");

            var lastDatetime = DateTime.MinValue;
            var last = ctrl.Find(new BsonDocument()
            {
                new BsonElement("_id", "asdasd")
            }, null).SingleOrDefault();

            if (last == null)
            {
                ctrl.InsertOne(new BsonDocument()
                {
                    new BsonElement("_id", "asdasd"),
                    new BsonElement("lastUpdated", DateTime.MinValue)
                });
            }
            else
            {
                lastDatetime = last["lastUpdated"].ToUniversalTime();
            }

            var collection = database.GetCollection<BsonDocument>("dx-com-item");

            var filter = Builders<BsonDocument>
                .Filter
                .Gte("updated", lastDatetime);
            var options = new FindOptions();
            var sort = Builders<BsonDocument>
                .Sort
                .Ascending("updated");

            //retrive the data from collection
            try
            {
                await collection.Find(filter, options)
                    .Sort(sort)
                    .ForEachAsync(x =>
                    {
                        //Console.WriteLine(x);
                        var r = elastic.IndexDocument(x.ToJson());
                    });
            }
            catch (MongoCommandException ex) when (ex.Code == 96)
            {
                // Command find failed: Executor error during find command: OperationFailed: Sort operation used more than the maximum 33554432 bytes of RAM. Add an index, or specify a smaller limit..
                Console.WriteLine("Creating index");
                await collection
                    .Indexes
                    .CreateOneAsync(Builders<BsonDocument>.IndexKeys.Ascending("updated"));
            }
        }
    }
}