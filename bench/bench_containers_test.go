package bench_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tccassandra "github.com/testcontainers/testcontainers-go/modules/cassandra"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"

	"cloud.google.com/go/firestore"

	cassandradriver "github.com/kirimatt/goncordia/driver/cassandra/v2"
	clickhousedriver "github.com/kirimatt/goncordia/driver/clickhouse/v2"
	dynamodbdriver "github.com/kirimatt/goncordia/driver/dynamodb/v2"
	firestoredriver "github.com/kirimatt/goncordia/driver/firestore/v2"
	mongodriver "github.com/kirimatt/goncordia/driver/mongodbv2/v2"
	pgxv5driver "github.com/kirimatt/goncordia/driver/pgxv5/v2"
	redisdriver "github.com/kirimatt/goncordia/driver/redis/v2"
	"github.com/kirimatt/goncordia/v2/driver"
)

var (
	benchPgxDriver        driver.Driver[pgx.Tx]
	benchMongoDriver      driver.Driver[*mongo.Session]
	benchRedisDriver      driver.Driver[redisdriver.NoTx]
	benchCassandraDriver  driver.Driver[cassandradriver.NoTx]
	benchClickHouseDriver driver.Driver[clickhousedriver.NoTx]
	benchDynamoDBDriver   driver.Driver[dynamodbdriver.NoTx]
	benchFirestoreDriver  driver.Driver[*firestore.Transaction]
)

// benchResets empties each backend so every benchmark starts from an empty
// store: rows left by an earlier benchmark would slow scans in the next one
// (the Firestore emulator and DynamoDB Local scan, so leftovers distort the
// numbers by an order of magnitude).
var benchResets = map[string]func() error{}

func resetBackend(b *testing.B, name string) {
	b.Helper()
	if reset := benchResets[name]; reset != nil {
		if err := reset(); err != nil {
			b.Fatalf("reset %s: %v", name, err)
		}
	}
}

func TestMain(m *testing.M) {
	ctx := context.Background()
	var cleanups []func()

	if d, cleanup := startPostgres(ctx); d != nil {
		benchPgxDriver = d
		cleanups = append(cleanups, cleanup)
	}
	if d, cleanup := startMongo(ctx); d != nil {
		benchMongoDriver = d
		cleanups = append(cleanups, cleanup)
	}
	if d, cleanup := startRedis(ctx); d != nil {
		benchRedisDriver = d
		cleanups = append(cleanups, cleanup)
	}
	if d, cleanup := startCassandra(ctx); d != nil {
		benchCassandraDriver = d
		cleanups = append(cleanups, cleanup)
	}
	if d, cleanup := startClickHouse(ctx); d != nil {
		benchClickHouseDriver = d
		cleanups = append(cleanups, cleanup)
	}
	if d, cleanup := startDynamoDB(ctx); d != nil {
		benchDynamoDBDriver = d
		cleanups = append(cleanups, cleanup)
	}
	if d, cleanup := startFirestore(ctx); d != nil {
		benchFirestoreDriver = d
		cleanups = append(cleanups, cleanup)
	}

	code := m.Run()

	for _, c := range cleanups {
		c()
	}
	os.Exit(code)
}

func startPostgres(ctx context.Context) (driver.Driver[pgx.Tx], func()) {
	ctr, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithDatabase("bench"),
		tcpostgres.WithUsername("bench"),
		tcpostgres.WithPassword("bench"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: skip postgres benchmarks: %v\n", err)
		return nil, nil
	}
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	d := pgxv5driver.New(pool)
	if err := d.Migrate(ctx); err != nil {
		pool.Close()
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	benchResets["postgres"] = func() error {
		_, err := pool.Exec(ctx, `TRUNCATE goncordia_jobs CASCADE`)
		return err
	}
	return d, func() {
		pool.Close()
		ctr.Terminate(ctx) //nolint:errcheck
	}
}

func startMongo(ctx context.Context) (driver.Driver[*mongo.Session], func()) {
	ctr, err := tcmongo.Run(ctx, "mongo:8.0", tcmongo.WithReplicaSet("rs0"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: skip mongodb benchmarks: %v\n", err)
		return nil, nil
	}
	uri, err := ctr.ConnectionString(ctx)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	if strings.Contains(uri, "?") {
		uri += "&directConnection=true"
	} else {
		uri += "?directConnection=true"
	}
	client, err := mongo.Connect(mongoopts.Client().ApplyURI(uri))
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	d, err := mongodriver.New(ctx, client, "bench")
	if err != nil {
		client.Disconnect(ctx) //nolint:errcheck
		ctr.Terminate(ctx)     //nolint:errcheck
		return nil, nil
	}
	if err := d.Migrate(ctx); err != nil {
		client.Disconnect(ctx) //nolint:errcheck
		ctr.Terminate(ctx)     //nolint:errcheck
		return nil, nil
	}
	benchResets["mongodb"] = func() error {
		if err := client.Database("bench").Drop(ctx); err != nil {
			return err
		}
		return d.Migrate(ctx)
	}
	return d, func() {
		client.Disconnect(ctx) //nolint:errcheck
		ctr.Terminate(ctx)     //nolint:errcheck
	}
}

func startRedis(ctx context.Context) (driver.Driver[redisdriver.NoTx], func()) {
	ctr, err := tcredis.Run(ctx, "redis:7")
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: skip redis benchmarks: %v\n", err)
		return nil, nil
	}
	addr, err := ctr.Endpoint(ctx, "")
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	rdb := goredis.NewClient(&goredis.Options{Addr: addr})
	d := redisdriver.New(rdb)
	if err := d.Migrate(ctx); err != nil {
		rdb.Close()        //nolint:errcheck
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	benchResets["redis"] = func() error {
		if err := rdb.FlushAll(ctx).Err(); err != nil {
			return err
		}
		return d.Migrate(ctx)
	}
	return d, func() {
		rdb.Close()        //nolint:errcheck
		ctr.Terminate(ctx) //nolint:errcheck
	}
}

func startCassandra(ctx context.Context) (driver.Driver[cassandradriver.NoTx], func()) {
	ctr, err := tccassandra.Run(ctx, "cassandra:4.1")
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: skip cassandra benchmarks: %v\n", err)
		return nil, nil
	}
	host, err := ctr.ConnectionHost(ctx)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	cluster := gocql.NewCluster(host)
	cluster.Timeout = 15 * time.Second
	cluster.ConnectTimeout = 15 * time.Second
	cluster.Consistency = gocql.Quorum

	sysSession, err := cluster.CreateSession()
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	if err := sysSession.Query(
		`CREATE KEYSPACE IF NOT EXISTS goncordia_bench
		 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`,
	).Exec(); err != nil {
		sysSession.Close()
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	sysSession.Close()

	cluster.Keyspace = "goncordia_bench"
	session, err := cluster.CreateSession()
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	d := cassandradriver.New(session)
	if err := d.Migrate(ctx); err != nil {
		session.Close()
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	benchResets["cassandra"] = func() error {
		iter := session.Query(`SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'goncordia_bench'`).Iter()
		var name string
		var tables []string
		for iter.Scan(&name) {
			tables = append(tables, name)
		}
		if err := iter.Close(); err != nil {
			return err
		}
		for _, t := range tables {
			if err := session.Query(`TRUNCATE ` + t).Exec(); err != nil {
				return err
			}
		}
		return nil
	}
	return d, func() {
		session.Close()
		ctr.Terminate(ctx) //nolint:errcheck
	}
}

func startClickHouse(ctx context.Context) (driver.Driver[clickhousedriver.NoTx], func()) {
	ctr, err := tcclickhouse.Run(ctx, "clickhouse/clickhouse-server:24.3-alpine")
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: skip clickhouse benchmarks: %v\n", err)
		return nil, nil
	}
	dsn, err := ctr.ConnectionString(ctx)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	d := clickhousedriver.New(conn)
	if err := d.Migrate(ctx); err != nil {
		conn.Close()
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	benchResets["clickhouse"] = func() error {
		rows, err := conn.Query(ctx, `SELECT name FROM system.tables WHERE database = currentDatabase() AND engine NOT LIKE '%View%'`)
		if err != nil {
			return err
		}
		var tables []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			tables = append(tables, name)
		}
		rows.Close()
		for _, t := range tables {
			if err := conn.Exec(ctx, "TRUNCATE TABLE "+t); err != nil {
				return err
			}
		}
		return nil
	}
	return d, func() {
		conn.Close()
		ctr.Terminate(ctx) //nolint:errcheck
	}
}

func startDynamoDB(ctx context.Context) (driver.Driver[dynamodbdriver.NoTx], func()) {
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "amazon/dynamodb-local:latest",
			ExposedPorts: []string{"8000/tcp"},
			WaitingFor: wait.ForHTTP("/").
				WithPort("8000/tcp").
				WithStatusCodeMatcher(func(code int) bool { return code >= 100 }),
		},
		Started: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: skip dynamodb benchmarks: %v\n", err)
		return nil, nil
	}
	addr, err := ctr.Endpoint(ctx, "")
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	svc := awsdynamodb.NewFromConfig(cfg, func(o *awsdynamodb.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", addr))
		o.EndpointDiscovery = awsdynamodb.EndpointDiscoveryOptions{
			EnableEndpointDiscovery: aws.EndpointDiscoveryDisabled,
		}
	})
	d := dynamodbdriver.New(svc)
	if err := d.Migrate(ctx); err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	benchResets["dynamodb"] = func() error {
		tables, err := svc.ListTables(ctx, &awsdynamodb.ListTablesInput{})
		if err != nil {
			return err
		}
		for _, t := range tables.TableNames {
			if _, err := svc.DeleteTable(ctx, &awsdynamodb.DeleteTableInput{TableName: aws.String(t)}); err != nil {
				return err
			}
		}
		return d.Migrate(ctx)
	}
	return d, func() { ctr.Terminate(ctx) } //nolint:errcheck
}

func startFirestore(ctx context.Context) (driver.Driver[*firestore.Transaction], func()) {
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators",
			ExposedPorts: []string{"8080/tcp"},
			Cmd: []string{
				"gcloud", "beta", "emulators", "firestore", "start",
				"--host-port=0.0.0.0:8080", "--project=bench",
			},
			WaitingFor: wait.ForListeningPort("8080/tcp").WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench: skip firestore benchmarks: %v\n", err)
		return nil, nil
	}
	addr, err := ctr.Endpoint(ctx, "")
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	if err := os.Setenv("FIRESTORE_EMULATOR_HOST", addr); err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		return nil, nil
	}
	client, err := firestore.NewClient(ctx, "bench")
	if err != nil {
		os.Unsetenv("FIRESTORE_EMULATOR_HOST") //nolint:errcheck
		ctr.Terminate(ctx)                     //nolint:errcheck
		return nil, nil
	}
	benchResets["firestore"] = func() error {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
			"http://"+addr+"/emulator/v1/projects/bench/databases/(default)/documents", nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close() //nolint:errcheck
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("clear firestore emulator: %s", resp.Status)
		}
		return nil
	}
	d := firestoredriver.New(client)
	return d, func() {
		client.Close()                         //nolint:errcheck
		os.Unsetenv("FIRESTORE_EMULATOR_HOST") //nolint:errcheck
		ctr.Terminate(ctx)                     //nolint:errcheck
	}
}

// ---- Enqueue ----

func BenchmarkEnqueue_Postgres(b *testing.B) {
	if benchPgxDriver == nil {
		b.Skip("postgres not available")
	}
	resetBackend(b, "postgres")
	benchmarkEnqueue(b, benchPgxDriver)
}

func BenchmarkEnqueue_MongoDB(b *testing.B) {
	if benchMongoDriver == nil {
		b.Skip("mongodb not available")
	}
	resetBackend(b, "mongodb")
	benchmarkEnqueue(b, benchMongoDriver)
}

func BenchmarkEnqueue_Redis(b *testing.B) {
	if benchRedisDriver == nil {
		b.Skip("redis not available")
	}
	resetBackend(b, "redis")
	benchmarkEnqueue(b, benchRedisDriver)
}

func BenchmarkEnqueue_Cassandra(b *testing.B) {
	if benchCassandraDriver == nil {
		b.Skip("cassandra not available")
	}
	resetBackend(b, "cassandra")
	benchmarkEnqueue(b, benchCassandraDriver)
}

func BenchmarkEnqueue_ClickHouse(b *testing.B) {
	if benchClickHouseDriver == nil {
		b.Skip("clickhouse not available")
	}
	resetBackend(b, "clickhouse")
	benchmarkEnqueue(b, benchClickHouseDriver)
}

func BenchmarkEnqueue_DynamoDB(b *testing.B) {
	if benchDynamoDBDriver == nil {
		b.Skip("dynamodb not available")
	}
	resetBackend(b, "dynamodb")
	benchmarkEnqueue(b, benchDynamoDBDriver)
}

func BenchmarkEnqueue_Firestore(b *testing.B) {
	if benchFirestoreDriver == nil {
		b.Skip("firestore not available")
	}
	resetBackend(b, "firestore")
	benchmarkEnqueue(b, benchFirestoreDriver)
}

// ---- EnqueueBatch(100) ----

func BenchmarkEnqueueBatch100_Postgres(b *testing.B) {
	if benchPgxDriver == nil {
		b.Skip("postgres not available")
	}
	resetBackend(b, "postgres")
	benchmarkEnqueueBatch(b, benchPgxDriver, 100)
}

func BenchmarkEnqueueBatch100_MongoDB(b *testing.B) {
	if benchMongoDriver == nil {
		b.Skip("mongodb not available")
	}
	resetBackend(b, "mongodb")
	benchmarkEnqueueBatch(b, benchMongoDriver, 100)
}

func BenchmarkEnqueueBatch100_Redis(b *testing.B) {
	if benchRedisDriver == nil {
		b.Skip("redis not available")
	}
	resetBackend(b, "redis")
	benchmarkEnqueueBatch(b, benchRedisDriver, 100)
}

func BenchmarkEnqueueBatch100_Cassandra(b *testing.B) {
	if benchCassandraDriver == nil {
		b.Skip("cassandra not available")
	}
	resetBackend(b, "cassandra")
	benchmarkEnqueueBatch(b, benchCassandraDriver, 100)
}

func BenchmarkEnqueueBatch100_ClickHouse(b *testing.B) {
	if benchClickHouseDriver == nil {
		b.Skip("clickhouse not available")
	}
	resetBackend(b, "clickhouse")
	benchmarkEnqueueBatch(b, benchClickHouseDriver, 100)
}

func BenchmarkEnqueueBatch100_DynamoDB(b *testing.B) {
	if benchDynamoDBDriver == nil {
		b.Skip("dynamodb not available")
	}
	resetBackend(b, "dynamodb")
	benchmarkEnqueueBatch(b, benchDynamoDBDriver, 100)
}

func BenchmarkEnqueueBatch100_Firestore(b *testing.B) {
	if benchFirestoreDriver == nil {
		b.Skip("firestore not available")
	}
	resetBackend(b, "firestore")
	benchmarkEnqueueBatch(b, benchFirestoreDriver, 100)
}

// ---- FetchAndComplete ----

func BenchmarkFetchAndComplete_Postgres(b *testing.B) {
	if benchPgxDriver == nil {
		b.Skip("postgres not available")
	}
	resetBackend(b, "postgres")
	benchmarkFetchAndComplete(b, benchPgxDriver)
}

func BenchmarkFetchAndComplete_MongoDB(b *testing.B) {
	if benchMongoDriver == nil {
		b.Skip("mongodb not available")
	}
	resetBackend(b, "mongodb")
	benchmarkFetchAndComplete(b, benchMongoDriver)
}

func BenchmarkFetchAndComplete_Redis(b *testing.B) {
	if benchRedisDriver == nil {
		b.Skip("redis not available")
	}
	resetBackend(b, "redis")
	benchmarkFetchAndComplete(b, benchRedisDriver)
}

func BenchmarkFetchAndComplete_Cassandra(b *testing.B) {
	if benchCassandraDriver == nil {
		b.Skip("cassandra not available")
	}
	resetBackend(b, "cassandra")
	benchmarkFetchAndComplete(b, benchCassandraDriver)
}

func BenchmarkFetchAndComplete_ClickHouse(b *testing.B) {
	if benchClickHouseDriver == nil {
		b.Skip("clickhouse not available")
	}
	resetBackend(b, "clickhouse")
	benchmarkFetchAndComplete(b, benchClickHouseDriver)
}

func BenchmarkFetchAndComplete_DynamoDB(b *testing.B) {
	if benchDynamoDBDriver == nil {
		b.Skip("dynamodb not available")
	}
	resetBackend(b, "dynamodb")
	benchmarkFetchAndComplete(b, benchDynamoDBDriver)
}

func BenchmarkFetchAndComplete_Firestore(b *testing.B) {
	if benchFirestoreDriver == nil {
		b.Skip("firestore not available")
	}
	resetBackend(b, "firestore")
	benchmarkFetchAndComplete(b, benchFirestoreDriver)
}

// ---- End-to-end ----

func BenchmarkEndToEnd_Postgres_c4(b *testing.B) {
	if benchPgxDriver == nil {
		b.Skip("postgres not available")
	}
	resetBackend(b, "postgres")
	benchmarkEndToEnd(b, benchPgxDriver, 4)
}

func BenchmarkEndToEnd_MongoDB_c4(b *testing.B) {
	if benchMongoDriver == nil {
		b.Skip("mongodb not available")
	}
	resetBackend(b, "mongodb")
	benchmarkEndToEnd(b, benchMongoDriver, 4)
}

func BenchmarkEndToEnd_Redis_c4(b *testing.B) {
	if benchRedisDriver == nil {
		b.Skip("redis not available")
	}
	resetBackend(b, "redis")
	benchmarkEndToEnd(b, benchRedisDriver, 4)
}

func BenchmarkEndToEnd_Cassandra_c4(b *testing.B) {
	if benchCassandraDriver == nil {
		b.Skip("cassandra not available")
	}
	resetBackend(b, "cassandra")
	benchmarkEndToEnd(b, benchCassandraDriver, 4)
}

func BenchmarkEndToEnd_ClickHouse_c4(b *testing.B) {
	if benchClickHouseDriver == nil {
		b.Skip("clickhouse not available")
	}
	resetBackend(b, "clickhouse")
	benchmarkEndToEnd(b, benchClickHouseDriver, 4)
}

func BenchmarkEndToEnd_DynamoDB_c4(b *testing.B) {
	if benchDynamoDBDriver == nil {
		b.Skip("dynamodb not available")
	}
	resetBackend(b, "dynamodb")
	benchmarkEndToEnd(b, benchDynamoDBDriver, 4)
}

func BenchmarkEndToEnd_Firestore_c4(b *testing.B) {
	if benchFirestoreDriver == nil {
		b.Skip("firestore not available")
	}
	resetBackend(b, "firestore")
	benchmarkEndToEndN(b, benchFirestoreDriver, 4, 200)
}
