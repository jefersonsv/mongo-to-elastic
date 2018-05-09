# mongo-to-elastic
Sync from Mongodb to Elasticsearch database

# Usage
Change both environment variables

```
git clone https://github.com/jefersonsv/mongo-to-elastic
cd mongo-to-elastic/src/mongo-to-elastic
sudo docker build -t mongo-to-elastic .
sudo docker run -d -e 'MONGODB=mongodb://simpleuser:simplepassword@127.0.0.1:27017' -e 'ELASTICSEARCH=http://127.0.0.1:9200' mongo-to-elastic
```