using Dapper;
using Elasticsearch.Net;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using Serilog;
using System;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using fastJSON;
using System.Text;
using CliWrap;
using System.IO;

namespace mongo_to_elastic
{
    public static class SqlServerToMongo
    {
        static long CountLinesInFile(string file)
        {
            var lineCount = 0;
            using (var reader = File.OpenText(file))
            {
                while (reader.ReadLine() != null)
                {
                    lineCount++;
                }
            }

            return lineCount;
        }

        static SqlConnection connection;

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
            var sqlserver = Environment.GetEnvironmentVariable("SQLSERVER") ?? "Data Source=H73V220I;Initial Catalog=st1643_1; User Id=user_st1643_1;Password=pwd_st1643_1";
            //var mongodb = Environment.GetEnvironmentVariable("MONGODB");

            //var clientMongo = new MongoClient(mongodb);
            //var databaseMongo = clientMongo.GetDatabase("dsv-digital");

            //JSON.Parameters.KVStyleStringDictionary = false;

            // Count lines
            var files = Directory.GetFiles(Environment.CurrentDirectory, "*.csv");

            //StringBuilder resume = new StringBuilder();

            //foreach (var item in files)
            //{
            //    resume.AppendLine($"File: {Path.GetFileName(item).PadRight(15, ' ')} Lines: {CountLinesInFile(item)}");
            //}

            //var asassad = resume.ToString();

            connection = GetOpenConnection(sqlserver);

            var tablesSqlserver = Environment.GetEnvironmentVariable("SQLSERVER_TABLES") ?? string.Empty;
            var tables = tablesSqlserver.Split(',', StringSplitOptions.RemoveEmptyEntries);

            if (!tables.Any())
            {
                // select all tables
                //var sql = @"SELECT CONCAT(SCHEMA_NAME(schema_id), '.', name) AS table_name FROM sys.tables ORDER BY 1 desc";

                //var tablesJson = JSON.ToJSON(connection.Query(sql));

                // tables

                var ignorelist = new string[] { "abc" };
                foreach (JObject tableJson in JArray.FromObject(connection.Query(tablesLineCount)))
                {
                    var tableName = tableJson.Value<string>("TableName");
                    var tableNameOnly = tableJson.Value<string>("TableNameOnly");

                    if (!tableNameOnly.Contains("not"))
                        continue;

                    // ignore list
                    if (ignorelist.Contains(tableNameOnly))
                        continue;

                    // file already exists
                    if (File.Exists(Path.Combine(Environment.CurrentDirectory, tableNameOnly + ".csv")))
                        continue;

                    Console.WriteLine("Exporting table: " + tableNameOnly);

                    try
                    {
                        var result = Cli.Wrap("bcp")
                            .SetArguments(string.Format(bcpTemplate, tableNameOnly))
                            .Execute();
                    }
                    catch (Exception ex)
                    {
                        if (File.Exists(Path.Combine(Environment.CurrentDirectory, tableNameOnly + ".csv")))
                            File.Delete(Path.Combine(Environment.CurrentDirectory, tableNameOnly + ".csv"));
                    }
                    /*
                    var columnsList = connection.Query<string>(string.Format(columns, tableNameOnly)).ToList();


                    StringBuilder sb = new StringBuilder();
                    sb.AppendLine("SELECT '" + string.Join("','", columnsList) + "'");
                    sb.AppendLine($"FROM {tableName}");
                    sb.AppendLine($"UNION ALL");
                    sb.AppendLine("SELECT TOP 10 " + string.Join(',', columnsList));
                    sb.AppendLine($"FROM {tableName}");
                    */




                    //var res = JArray.FromObject(connection.Query(sb.ToString()));

                    // create collection
                    //var collections = databaseMongo.ListCollectionNames().ToList();
                    //if (!collections.Contains(tableName))
                    //    databaseMongo.CreateCollection(tableName);

                    // get maxid
                    //var collectionMongo = databaseMongo.GetCollection<BsonDocument>(tableName);
                    //var result = collectionMongo.Find(new BsonDocument())
                    //    .Sort(new BsonDocument("$id", -1))
                    //    .FirstOrDefault();


                    //sb.AppendLine("select top 10 * from " + tableName);

                    ////if (result != null)
                    ////{
                    ////    sb.AppendLine(" WHERE Id > " + result["Id"].ToString());
                    ////}

                    //sb.AppendLine(" ORDER By Id ");

                    //// lines
                    //foreach (JObject contentJson in JArray.FromObject(connection.Query(sb.ToString())))
                    //{
                    //    BsonDocument doc = BsonDocument.Parse(contentJson.ToString());
                    //    collectionMongo.InsertOne(doc);




                    //}
                }



            }


            return;
            // Parameters
            var databaseName = "whole -cheap";
            var collectionName = "produtcs";
            var timelineField = "updated";
            var fieldDebug = "title";
            var needsDebug = !string.IsNullOrEmpty(fieldDebug);

            // Mongo
            var mongodb = "";
            var mongoClient = new MongoClient(mongodb);
            var database = mongoClient.GetDatabase(databaseName);
            var collection = database.GetCollection<BsonDocument>(collectionName);

            // Elastic Search
            var config = new Elasticsearch.Net.ConnectionConfiguration(new Uri(""));
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

        public static SqlConnection GetOpenConnection(string connectionString, bool mars = false)
        {
            if (mars)
            {
                var scsb = new SqlConnectionStringBuilder(connectionString)
                {
                    MultipleActiveResultSets = true
                };
                connectionString = scsb.ConnectionString;
            }
            var connection = new SqlConnection(connectionString);
            connection.Open();
            return connection;
        }

        public static string bcpTemplate = @" "" SELECT * FROM {0} "" queryout {0}.csv -b 1000 -c -S H73V220I -U user_st1643_1 -P pwd_st1643_1 -d st1643_1 ";

        public static string columns = @"SELECT	Quotename(COLUMN_NAME)
FROM	INFORMATION_SCHEMA.COLUMNS
WHERE	TABLE_NAME = '{0}'";

        public static string tablesLineCount = @"SELECT
              QUOTENAME(SCHEMA_NAME(sOBJ.schema_id)) + '.' + QUOTENAME(sOBJ.name) AS [TableName]
              , SUM(sPTN.Rows) AS [RowCount], sOBJ.name AS [TableNameOnly]
        FROM 
              sys.objects AS sOBJ
              INNER JOIN sys.partitions AS sPTN
                    ON sOBJ.object_id = sPTN.object_id
        WHERE
              sOBJ.type = 'U'
              AND sOBJ.is_ms_shipped = 0x0
              AND index_id < 2 -- 0:Heap, 1:Clustered
        GROUP BY 
              sOBJ.schema_id
              , sOBJ.name
        HAVING 
	        SUM(sPTN.Rows) > 0
        ORDER BY 2 asc
        ";

    }
}