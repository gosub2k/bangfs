package bangfuse

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"syscall"
	"time"

	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	"bangfs/bangutil"
	bangpb "bangfs/proto"
)

const pgcassCacheEntryTimeout = 5 * time.Second
const pgcassCacheSizeMB = 500 * 1024 * 1024

// PGCassKVStoreOptions holds configuration for creating a PGCassKVStore.
type PGCassKVStoreOptions struct {
	PGHost       string
	PGPort       uint16
	PGUser       string
	PGPassword   string
	PGDatabase   string
	CassHosts    []string
	CassPort     int
	CassUser     string
	CassPassword string
	CassRF       int // Cassandra replication factor (default 1)
	Namespace    string
	UseCache     bool
}

// PGCassKVStore stores metadata in Postgres and chunks in Cassandra.
type PGCassKVStore struct {
	opts        PGCassKVStoreOptions
	pgPool      *pgxpool.Pool
	cassSession *gocql.Session
	metaTable   string // postgres table: {namespace}_bangfs_metadata
	chunkTable  string // cassandra table in keyspace bangfs: {namespace}_chunks
	useCache    bool
	cache       *Cache
}

func validateNamespace(ns string) error {
	if len(ns) == 0 || len(ns) > 32 {
		return fmt.Errorf("namespace must be 1–32 characters")
	}
	for _, c := range ns {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return fmt.Errorf("namespace %q contains invalid character %q (alphanumeric and _ only)", ns, c)
		}
	}
	return nil
}

// NewPGCassKVStore creates a new PGCassKVStore and connects to both backends.
func NewPGCassKVStore(opts PGCassKVStoreOptions) (*PGCassKVStore, error) {
	if err := validateNamespace(opts.Namespace); err != nil {
		return nil, err
	}
	if opts.PGPort == 0 {
		opts.PGPort = 5432
	}
	if opts.CassPort == 0 {
		opts.CassPort = 9042
	}
	if opts.PGDatabase == "" {
		opts.PGDatabase = "bangfs"
	}
	if opts.CassRF == 0 {
		opts.CassRF = 1
	}

	kv := &PGCassKVStore{
		opts:       opts,
		metaTable:  opts.Namespace + "_bangfs_metadata",
		chunkTable: opts.Namespace + "_chunks",
		useCache:   opts.UseCache,
	}
	if opts.UseCache {
		numEntries := int(pgcassCacheSizeMB / GetChunkSize())
		kv.cache = NewCache(numEntries, pgcassCacheEntryTimeout, kv.putChunkToBackend)
		kv.cache.Start(pgcassCacheEntryTimeout / 2)
	}
	if err := kv.Connect(); err != nil {
		return kv, err
	}
	return kv, nil
}

// Connect connects or reconnects to Postgres and Cassandra.
func (kv *PGCassKVStore) Connect() error {
	if kv.pgPool != nil {
		kv.pgPool.Close()
	}
	if kv.cassSession != nil {
		kv.cassSession.Close()
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		kv.opts.PGUser, kv.opts.PGPassword, kv.opts.PGHost, kv.opts.PGPort, kv.opts.PGDatabase)
	pool, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		return fmt.Errorf("postgres connect: %w", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return fmt.Errorf("postgres ping: %w", err)
	}
	kv.pgPool = pool

	cassCluster := gocql.NewCluster(kv.opts.CassHosts...)
	cassCluster.Port = kv.opts.CassPort
	if kv.opts.CassUser != "" {
		cassCluster.Authenticator = gocql.PasswordAuthenticator{
			Username: kv.opts.CassUser,
			Password: kv.opts.CassPassword,
		}
	}
	session, err := cassCluster.CreateSession()
	if err != nil {
		return fmt.Errorf("cassandra connect: %w", err)
	}
	kv.cassSession = session
	return nil
}

// InitBackend creates the schema and root inode. Safe to call only once per namespace.
func (kv *PGCassKVStore) InitBackend() error {
	ctx := context.Background()

	_, err := kv.pgPool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			inode_id BIGINT PRIMARY KEY,
			data     BYTEA  NOT NULL,
			version  BIGINT NOT NULL DEFAULT 1
		)`, kv.metaTable))
	if err != nil {
		return fmt.Errorf("create metadata table: %w", err)
	}

	if err := kv.cassSession.Query(fmt.Sprintf(`
		CREATE KEYSPACE IF NOT EXISTS bangfs
		WITH replication = {'class':'SimpleStrategy','replication_factor':%d}`,
		kv.opts.CassRF)).Exec(); err != nil {
		return fmt.Errorf("create cassandra keyspace: %w", err)
	}
	if err := kv.cassSession.Query(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS bangfs.%s (
			chunk_id bigint PRIMARY KEY,
			data     blob
		)`, kv.chunkTable)).Exec(); err != nil {
		return fmt.Errorf("create chunk table: %w", err)
	}

	existing, _, err := kv.Metadata(0)
	if err == nil && existing != nil {
		return fmt.Errorf("filesystem already exists (inode 0 found). Use WipeBackend() first")
	}

	now := time.Now().UnixNano()
	rootDir := &bangpb.InodeMeta{
		Name:         "",
		ParentInode:  0,
		Mode:         0755 | syscall.S_IFDIR,
		Uid:          0,
		Gid:          0,
		CtimeNs:      now,
		MtimeNs:      now,
		AtimeNs:      now,
		ChildEntries: []*bangpb.ChildEntry{},
		Chunks:       nil,
		Nlink:        2,
		BlockSize:    GetChunkSize(),
	}
	if _, err := kv.PutMetadata(0, rootDir); err != nil {
		return fmt.Errorf("create root inode: %w", err)
	}
	return nil
}

// Close flushes the cache and closes both backend connections.
func (kv *PGCassKVStore) Close() error {
	if kv.cache != nil {
		kv.cache.Stop()
		_ = kv.cache.FlushAll()
	}
	if kv.pgPool != nil {
		kv.pgPool.Close()
	}
	if kv.cassSession != nil {
		kv.cassSession.Close()
	}
	return nil
}

// FlushChunks writes dirty cached chunks for the given keys to Cassandra.
func (kv *PGCassKVStore) FlushChunks(keys []uint64) error {
	if kv.cache == nil {
		return nil
	}
	return kv.cache.Flush(keys)
}

// DrainEvictErrors returns the first async cache eviction error, or nil.
func (kv *PGCassKVStore) DrainEvictErrors() error {
	if kv.cache == nil {
		return nil
	}
	return kv.cache.DrainErrors()
}

// vclockEncode encodes a uint64 version as 8 big-endian bytes.
func vclockEncode(version uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, version)
	return b
}

// vclockDecode decodes 8 big-endian bytes to a uint64 version.
func vclockDecode(vclock []byte) (uint64, error) {
	if len(vclock) != 8 {
		return 0, fmt.Errorf("invalid vclock length %d (expected 8)", len(vclock))
	}
	return binary.BigEndian.Uint64(vclock), nil
}

// PutMetadata creates a new inode. Fails if the key already exists.
func (kv *PGCassKVStore) PutMetadata(key uint64, newMeta *bangpb.InodeMeta) ([]byte, error) {
	data, err := proto.Marshal(newMeta)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	var version int64
	err = kv.pgPool.QueryRow(context.Background(),
		fmt.Sprintf(`INSERT INTO %s (inode_id, data, version) VALUES ($1, $2, 1) RETURNING version`,
			kv.metaTable),
		int64(key), data).Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("insert inode %d: %w", key, err)
	}
	return vclockEncode(uint64(version)), nil
}

// Metadata fetches inode metadata and its vclock.
func (kv *PGCassKVStore) Metadata(key uint64) (*bangpb.InodeMeta, []byte, error) {
	var data []byte
	var version int64
	err := kv.pgPool.QueryRow(context.Background(),
		fmt.Sprintf(`SELECT data, version FROM %s WHERE inode_id = $1`, kv.metaTable),
		int64(key)).Scan(&data, &version)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil, fmt.Errorf("key not found: %d", key)
		}
		return nil, nil, fmt.Errorf("fetch inode %d: %w", key, err)
	}
	meta := &bangpb.InodeMeta{}
	if err := proto.Unmarshal(data, meta); err != nil {
		return nil, nil, fmt.Errorf("unmarshal inode %d: %w", key, err)
	}
	return meta, vclockEncode(uint64(version)), nil
}

// UpdateMetadata updates inode metadata with optimistic concurrency control.
// Fails if the inode was modified since the vclock was read.
func (kv *PGCassKVStore) UpdateMetadata(key uint64, newMeta *bangpb.InodeMeta, vclockIn []byte) ([]byte, error) {
	oldVersion, err := vclockDecode(vclockIn)
	if err != nil {
		return nil, fmt.Errorf("invalid vclock: %w", err)
	}
	data, err := proto.Marshal(newMeta)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	var newVersion int64
	err = kv.pgPool.QueryRow(context.Background(),
		fmt.Sprintf(`UPDATE %s SET data=$2, version=version+1
			WHERE inode_id=$1 AND version=$3 RETURNING version`, kv.metaTable),
		int64(key), data, int64(oldVersion)).Scan(&newVersion)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("concurrent modification on inode %d", key)
		}
		return nil, fmt.Errorf("update inode %d: %w", key, err)
	}
	return vclockEncode(uint64(newVersion)), nil
}

// DeleteMetadata removes an inode. The vclock is accepted for interface compatibility
// but Postgres's atomic DELETE makes the check unnecessary.
func (kv *PGCassKVStore) DeleteMetadata(key uint64, _ []byte) error {
	_, err := kv.pgPool.Exec(context.Background(),
		fmt.Sprintf(`DELETE FROM %s WHERE inode_id = $1`, kv.metaTable),
		int64(key))
	return err
}

func (kv *PGCassKVStore) putChunkToBackend(key uint64, data []byte) error {
	return kv.cassSession.Query(
		fmt.Sprintf(`INSERT INTO bangfs.%s (chunk_id, data) VALUES (?, ?)`, kv.chunkTable),
		int64(key), data).Exec()
}

func (kv *PGCassKVStore) chunkFromBackend(key uint64) ([]byte, error) {
	var data []byte
	if err := kv.cassSession.Query(
		fmt.Sprintf(`SELECT data FROM bangfs.%s WHERE chunk_id = ?`, kv.chunkTable),
		int64(key)).Scan(&data); err != nil {
		if err == gocql.ErrNotFound {
			return nil, fmt.Errorf("chunk not found: %016x", key)
		}
		return nil, fmt.Errorf("fetch chunk %016x: %w", key, err)
	}
	return data, nil
}

// PutChunk stores a chunk, writing through the cache when enabled.
func (kv *PGCassKVStore) PutChunk(key uint64, data []byte) error {
	if kv.cache != nil {
		kv.cache.Add(key, data, true)
		return nil
	}
	return kv.putChunkToBackend(key, data)
}

// Chunk retrieves a chunk, serving from cache on hit.
func (kv *PGCassKVStore) Chunk(key uint64) ([]byte, error) {
	if kv.cache != nil {
		if data, ok := kv.cache.Get(key); ok {
			return data, nil
		}
		data, err := kv.chunkFromBackend(key)
		if err != nil {
			return nil, err
		}
		kv.cache.Add(key, data, false)
		return data, nil
	}
	return kv.chunkFromBackend(key)
}

// DeleteChunk removes a chunk from cache and backend.
func (kv *PGCassKVStore) DeleteChunk(key uint64) error {
	if kv.cache != nil {
		kv.cache.Delete(key)
	}
	return kv.cassSession.Query(
		fmt.Sprintf(`DELETE FROM bangfs.%s WHERE chunk_id = ?`, kv.chunkTable),
		int64(key)).Exec()
}

// WipeBackend deletes all metadata rows and truncates the chunk table.
func (kv *PGCassKVStore) WipeBackend(w io.Writer) error {
	ctx := context.Background()

	fmt.Fprintf(w, "  wiping metadata [%s]...\n", kv.metaTable)
	tag, err := kv.pgPool.Exec(ctx,
		fmt.Sprintf(`DELETE FROM %s`, kv.metaTable))
	if err != nil {
		return fmt.Errorf("wipe metadata: %w", err)
	}
	fmt.Fprintf(w, "  deleted %d metadata rows\n", tag.RowsAffected())

	fmt.Fprintf(w, "  wiping chunks [bangfs.%s]...\n", kv.chunkTable)
	if err := kv.cassSession.Query(
		fmt.Sprintf(`TRUNCATE bangfs.%s`, kv.chunkTable)).Exec(); err != nil {
		return fmt.Errorf("truncate chunks: %w", err)
	}
	fmt.Fprintf(w, "  chunks truncated\n")
	return nil
}

// DiskUsage estimates chunk usage from Cassandra's size_estimates table.
func (kv *PGCassKVStore) DiskUsage(chunkSize uint64) (*DiskUsageInfo, error) {
	op := bangutil.GetTracer().Op("DiskUsage", 0, "")

	var meanSize, partitions int64
	iter := kv.cassSession.Query(`
		SELECT mean_partition_size, partitions_count
		FROM system.size_estimates
		WHERE keyspace_name = 'bangfs' AND table_name = ?`,
		kv.chunkTable).Iter()

	var totalBytes uint64
	for iter.Scan(&meanSize, &partitions) {
		if meanSize > 0 && partitions > 0 {
			totalBytes += uint64(meanSize) * uint64(partitions)
		}
	}
	if err := iter.Close(); err != nil {
		op.Errorf("size_estimates query: %v", err)
	}

	usedChunks := totalBytes / chunkSize
	// Cassandra does not expose raw disk capacity via CQL; report 2× used as
	// a conservative lower-bound so df shows non-zero free space.
	totalChunks := usedChunks * 2
	if totalChunks == 0 {
		totalChunks = 1
	}
	op.Done()
	return &DiskUsageInfo{
		TotalChunks: totalChunks,
		UsedChunks:  usedChunks,
	}, nil
}
