ObjectDB

Huge-size object storage based on Cassandra with HTTP API.

Examples

curl -X PUT -H 'Content-Type: image/jpeg' -T ~/Downloads/earth.jpg http://127.0.0.1:7070/test/earth.jpg

curl -v http://127.0.0.1:7070/test/earth.jpg