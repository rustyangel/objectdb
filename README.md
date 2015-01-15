ObjectDB

Huge-size object storage based on Cassandra with HTTP API.

Build

go get -d

go build

Usage

bash-3.2$ ./objectdb --help
Usage of ./objectdb:
  -ch="localhost": cassandra comma-separated hosts
  -ck="objectdb_test": cassandra keyspace
  -l="0.0.0.0:7070": http api listen addr host:port 

Examples

curl -X PUT -H 'Content-Type: image/jpeg' -T ~/Downloads/earth.jpg http://127.0.0.1:7070/test/earth.jpg

curl -v http://127.0.0.1:7070/test/earth.jpg