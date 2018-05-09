using MongoDB.Bson;
using MongoDB.Bson.IO;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace mongo_to_elastic
{
    public static class JsonHelper
    {
        public static string Bson2Json(BsonDocument bson)
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
                    o[o.SelectToken(item.Parent.Path).Path] = DateTimeHelper.JavaTimeStampToDateTime(d).ToString("o");
                });

            return o.ToString();
        }

        public static string RemoveElasticMetaFields(string json)
        {
            var obj = JObject.Parse(json);

            // https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-fields.html#_indexing_meta_fields
            // Remove meta fields
            obj.Remove("_id");
            obj.Remove("_type");

            return obj.ToString();
        }
    }
}