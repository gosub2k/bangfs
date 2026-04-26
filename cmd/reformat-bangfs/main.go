// rmbangfs destroys a BangFS filesystem in the backend
// WARNING: This permanently deletes all data!
package main

import (
	"bufio"
	"flag"
	"fmt"
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

	// Common flags
	namespace := flag.String("namespace", envOrDefault("BANGFS_NAMESPACE", ""), "Filesystem namespace (env: BANGFS_NAMESPACE)")
	dummy := flag.Bool("dummy", false, "Use file-backed store under /tmp instead of real backends")
	force := flag.Bool("force", false, "Skip confirmation prompt")

	flag.Parse()

	if *namespace == "" {
		log.Println("Error: -namespace is required (or set BANGFS_NAMESPACE)")
		flag.Usage()
		os.Exit(1)
	}

	if !*force {
		fmt.Printf("WARNING: This will permanently delete all data in namespace '%s'!\n", *namespace)
		fmt.Printf("  Metadata table: %s_bangfs_metadata (Postgres)\n", *namespace)
		fmt.Printf("  Chunk table:    bangfs.%s_chunks (Cassandra)\n", *namespace)
		fmt.Print("\nType the namespace name to confirm: ")

		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input != *namespace {
			log.Fatal("Confirmation failed. Aborting.")
		}
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
			Namespace:    *namespace,
		})
		if err != nil {
			log.Fatalf("Failed to connect to backend: %v", err)
		}
		kv = pkv
	}
	defer kv.Close()

	log.Printf("Wiping filesystem with namespace '%s'...", *namespace)
	if err := kv.WipeBackend(os.Stderr); err != nil {
		log.Fatalf("Failed to wipe filesystem: %v", err)
	}
	log.Printf("Filesystem wiped successfully")
}
