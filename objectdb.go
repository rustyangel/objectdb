package main

import "net/http"
import "fmt"
import "log"
import "strconv"
import "github.com/gocql/gocql"
import "strings"
import "crypto/md5"
import "encoding/hex"
import "time"
import _ "net/http/pprof"
import "bufio"
import "bytes"
import "io"
import "runtime"
import "flag"

const (
	ChunkMaxSize = 1024 * 1024 * 8
)

var (
	flListendAddr       = flag.String("l", "0.0.0.0:7070", "http api listen addr host:port")
	flCassandraHosts    = flag.String("ch", "localhost", "cassandra comma-separated hosts")
	flCassandraKeyspace = flag.String("ck", "objectdb_test", "cassandra keyspace")
)

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	odb := NewObjectDB()
	cassandraHosts := strings.Split(*flCassandraHosts, ",")
	log.Printf("connect to cassandra keyspace = %s, hosts = %s", *flCassandraKeyspace, cassandraHosts)
	err := odb.ConnectDB(*flCassandraKeyspace, cassandraHosts...)
	if err != nil {
		log.Fatalf("connect to db err: %s", err)
	}
	log.Printf("http api start on %s", *flListendAddr)
	err = http.ListenAndServe(*flListendAddr, odb)
	if err != nil {
		log.Fatalf("http server start err: %s", err)
	}
}

type ObjectDB struct {
	csSession *gocql.Session
}

func NewObjectDB() (odb *ObjectDB) {
	odb = new(ObjectDB)
	return odb
}

func (odb *ObjectDB) ConnectDB(keyspace string, hosts ...string) (err error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.LocalQuorum
	odb.csSession, err = cluster.CreateSession()
	return
}

func (odb ObjectDB) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	log.Printf("request %s %s", req.Method, req.URL.Path)
	defer func() {
		if r := recover(); r != nil {
			log.Printf("err: %s", r)
			rw.WriteHeader(500)
			fmt.Fprintf(rw, "err: %s\n", r)
		}
	}()
	switch req.Method {
	case "PUT":
		odb.dataPut(rw, req)
	case "GET":
		odb.dataGet(rw, req)
	default:
		rw.WriteHeader(405)
		panic(fmt.Sprintf("Invalid Method: %s", req.Method))
	}
}

func (odb *ObjectDB) dataGet(rw http.ResponseWriter, req *http.Request) {
	pathParts := strings.Split(req.URL.Path, "/")
	if len(pathParts) < 3 {
		panic("bad bucket and object name")
	}
	bucket := pathParts[1]
	objectName := strings.Join(pathParts[2:], "/")
	log.Printf("get object %s::%s", bucket, objectName)
	var length int64
	var metadata map[string]string
	var created int64
	err := odb.csSession.Query("SELECT content_length, created, metadata FROM objects WHERE bucket = ? and name = ?",
		bucket, objectName).Scan(&length, &created, &metadata)
	if err != nil {
		if err == gocql.ErrNotFound {
			rw.WriteHeader(404)
			fmt.Fprintf(rw, "Object Not Found\n")
			return
		}
		panic(err)
	}
	rw.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	contentType := metadata["content-type"]
	if contentType != "" {
		rw.Header().Set("Content-Type", contentType)
	}
	chunkCount := length / ChunkMaxSize
	if length%ChunkMaxSize > 0 {
		chunkCount++
	}
	var data []byte
	rw.WriteHeader(200)
	for chunkNum := int64(0); chunkNum < chunkCount; chunkNum++ {
		err = odb.csSession.Query("SELECT data FROM chunks WHERE bucket = ? AND name = ? AND num = ?", bucket, objectName, chunkNum).Scan(&data)
		if err != nil {
			panic(err)
		}
		log.Printf("transfer chunk %s::%s::%d len %d, %s", bucket, objectName, chunkNum, len(data), hex.EncodeToString(data)[:100])
		rw.Write(data)
	}
}

func (odb *ObjectDB) dataPut(rw http.ResponseWriter, req *http.Request) {
	pathParts := strings.Split(req.URL.Path, "/")
	if len(pathParts) < 3 {
		panic("bad bucket and object name")
	}
	bucket := pathParts[1]
	objectName := strings.Join(pathParts[2:], "/")
	var objectExist int
	err := odb.csSession.Query("SELECT count(*) FROM objects WHERE bucket = ? and name = ?", bucket, objectName).Scan(&objectExist)
	if err != nil {
		panic(err)
	}
	if objectExist == 1 {
		panic("object exist")
	}
	contentLengthHeader := req.Header.Get("Content-Length")
	if contentLengthHeader == "" {
		panic("missing header Content-Length")
	}
	contentLength, err := strconv.ParseInt(contentLengthHeader, 10, 64)
	if err != nil {
		panic(err)
	}
	contentType := req.Header.Get("Content-Type")
	contentEncoding := req.Header.Get("Content-Encoding")
	chunkCount := contentLength / ChunkMaxSize
	if contentLength%ChunkMaxSize > 0 {
		chunkCount++
	}
	created := time.Now().UTC().Unix()
	log.Printf("put begin %s::%s with length %d (chunks %d)", bucket, objectName, contentLength, chunkCount)
	defer func() {
		log.Printf("put end %s::%s with length %d (chunks %d), speed %d msec", bucket, objectName, contentLength, chunkCount,
			time.Now().UTC().Unix()-created)
	}()
	metadata := make(map[string]string)
	if contentType != "" {
		metadata["content-type"] = contentType
	}
	if contentEncoding != "" {
		metadata["content-encoding"] = contentEncoding
	}
	err = odb.csSession.Query("INSERT INTO objects (bucket, name, content_length, created, metadata) VALUES (?, ?, ?, dateOf(now()), ?)",
		bucket, objectName, contentLength, metadata).Exec()
	if err != nil {
		panic(err)
	}
	var chunk = &bytes.Buffer{}
	reader := bufio.NewReaderSize(req.Body, 1024*128)
	chunkNum := 0
	for {
		n, err := io.CopyN(chunk, reader, ChunkMaxSize)
		if err != nil && err != io.EOF {
			panic(err)
		}
		log.Printf("readed %d bytes", n)
		if (chunk.Len() >= ChunkMaxSize) || err == io.EOF {
			odb.storeChunk(bucket, objectName, chunk, chunkNum)
			chunkNum++
		}
		if err == io.EOF {
			break
		}
	}
}

func (odb *ObjectDB) storeChunk(bucket, objectName string, chunk *bytes.Buffer, chunkNum int) (err error) {
	if chunk.Len() == 0 {
		return
	}
	log.Printf("store chunk %s::%s (%d bytes)", bucket, objectName, chunk.Len())
	if err = odb.csSession.Query("INSERT INTO chunks (bucket, name, num, data) VALUES (?, ?, ?, ?)",
		bucket, objectName, chunkNum, chunk.Bytes()).Exec(); err != nil {
		return
	}
	chunk.Reset()
	return
}

func hashstr(sdata ...[]byte) string {
	hash := md5.New()
	for _, sitem := range sdata {
		hash.Write(sitem)
	}
	md := hash.Sum(nil)
	return hex.EncodeToString(md)
}
