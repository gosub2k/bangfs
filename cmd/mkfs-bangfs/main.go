// mkbangfs initializes a new BangFS filesystem in the backend
package main

import (
	"flag"
	"log"
	"os"
	"strconv"
	"strings"

	"bangfs/bangfuse"
)

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envUintOrDefault(key string, fallback uint) uint {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil {
			return uint(n)
		}
	}
	return fallback
}

func main() {
	// Postgres flags
	pgHost := flag.String("pg-host", envOrDefault("POSTGRES_HOST", ""), "Postgres host (env: POSTGRES_HOST)")
	pgPort := flag.Uint("pg-port", envUintOrDefault("POSTGRES_PORT", 5432), "Postgres port (env: POSTGRES_PORT)")
	pgUser := flag.String("pg-user", envOrDefault("POSTGRES_USER", "bangfs"), "Postgres user (env: POSTGRES_USER)")
	pgPassword := flag.String("pg-password", envOrDefault("POSTGRES_PASSWORD", ""), "Postgres password (env: POSTGRES_PASSWORD)")
	pgDB := flag.String("pg-db", envOrDefault("POSTGRES_DB", "bangfs"), "Postgres database (env: POSTGRES_DB)")

	// Cassandra flags
	cassHostsStr := flag.String("cass-hosts", envOrDefault("CASSANDRA_HOSTS", ""), "Cassandra hosts, comma-separated (env: CASSANDRA_HOSTS)")
	cassPort := flag.Uint("cass-port", envUintOrDefault("CASSANDRA_PORT", 9042), "Cassandra port (env: CASSANDRA_PORT)")
	cassUser := flag.String("cass-user", envOrDefault("CASSANDRA_USER", ""), "Cassandra user (env: CASSANDRA_USER)")
	cassPassword := flag.String("cass-password", envOrDefault("CASSANDRA_PASSWORD", ""), "Cassandra password (env: CASSANDRA_PASSWORD)")
	cassRF := flag.Uint("cass-rf", envUintOrDefault("CASSANDRA_RF", 1), "Cassandra replication factor (env: CASSANDRA_RF)")

	// Common flags
	namespace := flag.String("namespace", envOrDefault("BANGFS_NAMESPACE", ""), "Filesystem namespace (env: BANGFS_NAMESPACE)")
	dummy := flag.Bool("dummy", false, "Use file-backed store under /tmp instead of real backends")
	chunkSize := flag.Uint("chunk-size", 1024*1024, "Chunk size in bytes (default 1MB)")

	flag.Parse()

	bangfuse.SetChunkSize(uint64(*chunkSize))

	if *namespace == "" {
		log.Println("Error: -namespace is required (or set BANGFS_NAMESPACE)")
		flag.Usage()
		os.Exit(1)
	}

	var kv bangfuse.KVStore
	if *dummy {
		log.Printf("Using file-backed store (namespace=%s)", *namespace)
		fkv, err := bangfuse.NewFileKVStore(*namespace)
		if err != nil {
			log.Fatalf("Failed to create file store: %v", err)
		}
		kv = fkv
	} else {
		if *pgHost == "" || *cassHostsStr == "" {
			log.Println("Error: -pg-host and -cass-hosts are required (or set POSTGRES_HOST, CASSANDRA_HOSTS), or use -dummy")
			flag.Usage()
			os.Exit(1)
		}
		cassHosts := strings.Split(*cassHostsStr, ",")
		log.Printf("Connecting to Postgres at %s:%d and Cassandra at %s", *pgHost, *pgPort, *cassHostsStr)
		pkv, err := bangfuse.NewPGCassKVStore(bangfuse.PGCassKVStoreOptions{
			PGHost:       *pgHost,
			PGPort:       uint16(*pgPort),
			PGUser:       *pgUser,
			PGPassword:   *pgPassword,
			PGDatabase:   *pgDB,
			CassHosts:    cassHosts,
			CassPort:     int(*cassPort),
			CassUser:     *cassUser,
			CassPassword: *cassPassword,
			CassRF:       int(*cassRF),
			Namespace:    *namespace,
			UseCache:     false,
		})
		if err != nil {
			log.Fatalf("Failed to connect to backend: %v", err)
		}
		kv = pkv
	}
	defer kv.Close()

	log.Printf("Initializing filesystem with namespace '%s'", *namespace)
	if err := kv.InitBackend(); err != nil {
		log.Fatalf("Failed to initialize filesystem: %v", err)
	}

	log.Printf("Filesystem initialized successfully!")
	log.Printf("  Metadata table: %s_bangfs_metadata (Postgres)", *namespace)
	log.Printf("  Chunk table:    bangfs.%s_chunks (Cassandra)", *namespace)
	log.Printf("  Chunk size:     %d bytes", *chunkSize)
}
