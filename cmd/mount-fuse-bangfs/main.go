// bangfs mounts a BangFS filesystem
package main

import (
	"flag"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"bangfs/bangfuse"
	"bangfs/bangutil"
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
	mountpoint := flag.String("mount", envOrDefault("BANGFS_MOUNTDIR", ""), "Mount point (env: BANGFS_MOUNTDIR)")
	dummy := flag.Bool("dummy", false, "Use file-backed store under /tmp instead of real backends")
	nocache := flag.Bool("nocache", false, "Disable write-back chunk cache")
	daemon := flag.Bool("daemon", false, "Run in background (daemon mode)")
	daemonChild := flag.Bool("daemon-child", false, "Internal flag for daemon mode")
	trace := flag.Bool("trace", false, "Enable tracing output for debugging")
	tracedebug := flag.Bool("tracedebug", false, "Enable debug-level tracing (implies -trace)")
	tracelog := flag.String("tracelog", "", "Write trace output to file instead of stderr")

	flag.Parse()

	if *trace || *tracedebug {
		tracer := bangutil.GetTracer()
		if *tracelog != "" {
			if err := tracer.SetOutputFile(*tracelog); err != nil {
				log.Fatalf("Failed to open trace log: %v", err)
			}
			defer tracer.CloseOutput()
		}
		tracer.Enable()
		if *tracedebug {
			tracer.SetLevel(bangutil.LevelDebug)
		}
	}

	if *namespace == "" || *mountpoint == "" {
		log.Println("Error: -namespace and -mount are required (or set BANGFS_NAMESPACE, BANGFS_MOUNTDIR)")
		flag.Usage()
		os.Exit(1)
	}

	if *daemon && !*daemonChild {
		args := append(os.Args[1:], "-daemon-child")
		cmd := exec.Command(os.Args[0], args...)
		cmd.Stdout = nil
		cmd.Stderr = nil
		cmd.Stdin = nil
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := cmd.Start(); err != nil {
			log.Fatalf("Failed to start daemon: %v", err)
		}
		log.Printf("BangFS daemon started (pid %d)", cmd.Process.Pid)
		os.Exit(0)
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
			UseCache:     !*nocache,
		})
		if err != nil {
			log.Fatalf("Failed to connect to backend: %v", err)
		}
		kv = pkv
	}

	bs, err := bangfuse.NewBangServer(kv)
	if err != nil {
		log.Fatalf("Failed to initialize: %v", err)
	}
	defer bs.Close()

	log.Printf("Mounting BangFS (namespace=%s) at %s (chunk size: %d bytes)", *namespace, *mountpoint, bangfuse.GetChunkSize())
	if err := bs.Mount(*mountpoint); err != nil {
		log.Fatalf("Mount failed: %v", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received %v, unmounting...", sig)
		if err := bs.Unmount(); err != nil {
			log.Printf("Unmount error: %v", err)
		}
	}()

	bs.Wait()
	log.Println("Unmounted successfully")
}
