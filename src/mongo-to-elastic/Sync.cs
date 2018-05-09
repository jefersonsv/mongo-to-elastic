using Elasticsearch.Net;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using Serilog;
using System;
using System.Threading.Tasks;

namespace mongo_to_elastic
{
    public static class Sync
    {
        public static DateTime GetLastDateTimeSerie(string databaseName, string collectionName, string timelineField, ElasticLowLevelClient lowLevalClient)
        {
            JObject json = new JObject(
                new JProperty("aggs", new JObject() {
                    new JProperty("lastUpdated", new JObject() {
                        new JProperty("max",  new JObject() {
                            new JProperty("field", timelineField)
                        })
                    })
                })
            );

            SearchRequestParameters param = new SearchRequestParameters();
            param.SetQueryString("size", 0);

            var search = lowLevalClient.Search<StringResponse>(databaseName, collectionName, json.ToString(), param);

            if (search.Success)
            {
                var jsonResponse = JObject.Parse(search.Body);
                var longTicks = jsonResponse.SelectToken("aggregations.lastUpdated.value");

                if (!string.IsNullOrEmpty(longTicks.ToString()))
                {
                    return DateTimeHelper.JavaTimeStampToDateTime(longTicks.Value<double>());
                }
            }

            return DateTime.MinValue;
        }

        public static async Task Start()
        {
            Log.Information("Starting job");

            // Configuration
            var mongodb = Environment.GetEnvironmentVariable("MONGODB");
            var elasticsearch = Environment.GetEnvironmentVariable("ELASTICSEARCH");

            // Parameters
            var databaseName = "whole-cheap";
            var collectionName = "produtcs";
            var timelineField = "updated";
            var fieldDebug = "title";
            var needsDebug = !string.IsNullOrEmpty(fieldDebug);

            // Mongo
            var mongoClient = new MongoClient(mongodb);
            var database = mongoClient.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);

            // Elastic Search
            var config = new Elasticsearch.Net.ConnectionConfiguration(new Uri(elasticsearch));
            var lowLevalClient = new Elasticsearch.Net.ElasticLowLevelClient(config);

            Log.Information($"Mongo parameters:");
            Log.Information($"  Database: {databaseName}");
            Log.Information($"  Collection: {collectionName}");
            Log.Information($"Elastic parameters:");
            Log.Information($"  Database: {databaseName}");
            Log.Information($"  Collection: {collectionName}");
            Log.Information($"Monto to Elastic parameters:");
            if (needsDebug)
                Log.Information($"  Field Debug: {fieldDebug}");

            //lowLevalClient.IndicesDelete<StringResponse>(databaseName);

            var exist = lowLevalClient.IndicesExists<StringResponse>(databaseName);
            if (exist.HttpStatusCode == 404)
            {
                Log.Warning($"Missing elastic index {collectionName}. Creating index...");
                var index = lowLevalClient.IndicesCreate<StringResponse>(databaseName, null);
            }

            var lastDatetime = GetLastDateTimeSerie(databaseName, collectionName, timelineField, lowLevalClient);
            Log.Information($"Last date time synced {lastDatetime}");

            var builder = Builders<BsonDocument>.Filter;
            var filter = builder.Gte(timelineField, lastDatetime) & builder.Lte(timelineField, DateTime.UtcNow);
            var sort = Builders<BsonDocument>.Sort
                .Ascending(timelineField);

            try
            {
                var total = await collection.Find(filter).CountAsync();
                Log.Information($"{total} documents should be indexed");

                await collection.Find(filter)
                    .Sort(sort)
                    .ForEachAsync(async x =>
                    {
                        var json = JsonHelper.RemoveElasticMetaFields(JsonHelper.Bson2Json(x));

                        var id = x["_id"].ToString();
                        var dbg = x[fieldDebug].ToString();
                        var timeLine = x[timelineField].ToString();

                        var response = await lowLevalClient.IndexAsync<StringResponse>(databaseName, collectionName, id, json);
                        var responseJson = JObject.Parse(response.Body);

                        if (response.Success)
                        {
                            // updated or inserted
                            string resultResponse = responseJson.Property("result").Value.ToString();

                            // Created
                            if (needsDebug)
                                Log.Debug($"{resultResponse} ID {id} [{timeLine}]: {dbg}");
                        }
                        else
                        {
                            Log.Error($"ID {id} error {dbg} reason {response.DebugInformation}");
                        }
                    });
            }
            catch (MongoCommandException ex) when (ex.Code == 96)
            {
                // Command find failed: Executor error during find command: OperationFailed: Sort operation used more than the maximum 33554432 bytes of RAM. Add an index, or specify a smaller limit..
                Log.Warning($"Missing mongo index of field {timelineField} on {collectionName}. Creating index...");
                await collection
                    .Indexes
                    .CreateOneAsync(Builders<BsonDocument>.IndexKeys.Ascending(timelineField));
            }

            Log.Information("Ending job");
        }
    }
}