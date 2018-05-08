using Elasticsearch.Net;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace mongo_to_elastic
{
    internal class Program
    {
        /// <summary>
        /// https://stackoverflow.com/questions/249760/how-can-i-convert-a-unix-timestamp-to-datetime-and-vice-versa
        /// </summary>
        /// <param name="javaTimeStamp"></param>
        /// <returns></returns>
        public static DateTime JavaTimeStampToDateTime(double javaTimeStamp)
        {
            // Java timestamp is milliseconds past epoch
            System.DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddMilliseconds(javaTimeStamp).ToLocalTime();
            return dtDateTime;
        }

        public static DateTime UnixTimeStampToDateTime(string unixTimeStamp)
        {
            double timeStamp = Double.Parse(unixTimeStamp);
            // Unix timestamp is seconds past epoch
            System.DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);
            dtDateTime = dtDateTime.AddSeconds(timeStamp).ToLocalTime();
            return dtDateTime;
        }

        private static string Bson2Json(BsonDocument bson)
        {
            var strict = bson.ToJson(new JsonWriterSettings { OutputMode = JsonOutputMode.Strict });

            var o = JObject.Parse(strict);
            //if (!string.IsNullOrEmpty(fieldId))
            //  o.Add(fieldId, o.SelectToken("_id.$oid"));

            // https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-fields.html#_indexing_meta_fields
            // Remove meta fields
            o.Remove("_id");
            o.Remove("_type");

            // Change datetime
            o.Descendants()
                .Where(o1 => o1.Type == JTokenType.Property)
                .Cast<JProperty>()
                .Where(o2 => o2.Name == "$date")
                .ToList()
                .ForEach(item =>
                {
                    //"2015-01-01T12:10:30Z"
                    //o[o.SelectToken(item.Parent.Path).Path] = item.Value;
                    double d = double.Parse(item.Value.ToString());
                    o[o.SelectToken(item.Parent.Path).Path] = JavaTimeStampToDateTime(d).ToString("o");
                });

            return o.ToString();
        }

        private static void Main(string[] args)
        {
            Start(args).Wait();
            //Console.ReadLine();
            Console.WriteLine("Finish");
        }

        private static async Task Start(string[] args)
        {
            // Configuration
            var mongodb = Environment.GetEnvironmentVariable("MONGODB");
            var elasticsearch = Environment.GetEnvironmentVariable("ELASTICSEARCH");

            // Parameters
            var databaseName = "whole-cheap";
            var collectionName = "dx-com-item";
            var debugField = "title";
            var timelineField = "updated";

            // Mongo
            var mongoClient = new MongoClient(mongodb);
            var database = mongoClient.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);

            //// Elastic Search
            //
            //var elasticClient = new Elasticsearch.Net.ElasticLowLevelClient(config);
            //var a = elasticClient.IndicesExists(databaseName);

            // Nest
            var settings = new Nest.ConnectionSettings(new Uri(elasticsearch))
                .DisableDirectStreaming(true)
                .DefaultIndex(databaseName);

            var elasticClient = new Nest.ElasticClient(settings);
            elasticClient.DeleteIndex(databaseName);

            var config = new Elasticsearch.Net.ConnectionConfiguration(new Uri(elasticsearch));
            var lowLevalClient = new Elasticsearch.Net.ElasticLowLevelClient(config);

            if (!elasticClient.IndexExists(databaseName).Exists)
            {
                // get sample to map
                //var sample = collection.Find(new BsonDocument()).FirstOrDefault();
                //var s = new Elasticsearch.Net.SerializableData<BsonDocument>(sample);

                var index = elasticClient.CreateIndex(databaseName);
                // index
                //var i = elastic.CreateIndex(databaseName, c => c
                //    .Mappings(ms => ms.Map<JObject>(collectionName, ms2 => ms2.AutoMap(o.GetType())))
                //);
            }

            var lastDatetime = DateTime.MinValue;

            JObject qag = new JObject(
                    new JProperty("aggs", new JObject() {
                            new JProperty("updated", new JObject() {
                                new JProperty("max",  new JObject() {
                                        new JProperty("field", timelineField)
                                    })
                            })
                        })
                    );

            SearchRequestParameters param = new SearchRequestParameters();
            param.SetQueryString("size", 0);

            var aa = lowLevalClient.Search<StringResponse>(databaseName, collectionName, qag.ToString(), param);
            // Check last updated
            var ss = elasticClient.Search<BsonDocument>(s => s
                .StoredFields(sf => sf
                    .Fields(timelineField)
                )
                .Query(q => q
                    .MatchAll()
                ));
            /*
            var ctrl = database.GetCollection<BsonDocument>("__mongo-to-elastic");

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
            }*/

            var builder = Builders<BsonDocument>.Filter;
            var filter = builder.Gte(timelineField, lastDatetime) & builder.Lte(timelineField, DateTime.UtcNow);
            var sort = Builders<BsonDocument>.Sort
                .Ascending(timelineField);

            try
            {
                await collection.Find(filter)
                    .Sort(sort)
                    .ForEachAsync(async x =>
                    {
                        var json = Bson2Json(x);

                        var id = x["_id"].ToString();
                        var dbg = x[debugField].ToString();

                        if (dbg.Length > 70)
                            dbg = dbg.Substring(0, 70);

                        var response = await lowLevalClient.IndexAsync<StringResponse>(databaseName, collectionName, id, json);
                        //dynamic reponseDin = Jil.JSON.DeserializeDynamic(response.Body);
                        var responseJson = JObject.Parse(response.Body);

                        if (response.Success)
                        {
                            // updated or inserted
                            string resultResponse = responseJson.Property("result").Value.ToString();

                            //BsonElement be = new BsonElement();
                            //if (!x.TryGetElement(aliasElasticId, out be))
                            //{
                            //    string elasticID = responseJson.Property("_id").Value.ToString();
                            //    x.Add(new BsonElement(aliasElasticId, elasticID));
                            //    //collection.fin<BsonDocument>(fi)
                            //    var writeElasticID = collection.UpdateOneAsync(
                            //            Builders<BsonDocument>.Filter.Eq("_id", id),
                            //            new BsonDocument(aliasElasticId, elasticID)
                            //        );
                            //    writeElasticID.Wait();
                            //}

                            // Update mongo

                            // Created
                            Console.WriteLine($"{resultResponse}: {dbg}");
                        }
                        else
                        {
                            Console.WriteLine("Err: " + dbg);
                        }
                    });
            }
            catch (MongoCommandException ex) when (ex.Code == 96)
            {
                // Command find failed: Executor error during find command: OperationFailed: Sort operation used more than the maximum 33554432 bytes of RAM. Add an index, or specify a smaller limit..
                Console.WriteLine("Creating index");
                await collection
                    .Indexes
                    .CreateOneAsync(Builders<BsonDocument>.IndexKeys.Ascending(timelineField));
            }
        }
    }
}