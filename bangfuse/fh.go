package bangfuse

import (
	"bangfs/bangutil"
	"context"
	"fmt"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	bangpb "bangfs/proto"
)

type BangFH struct {
	fs.FileHandle
	Flags    uint32
	Inum     uint64
	Metadata *bangpb.InodeMeta
	VClock   []byte
}

func (f *BangFH) String() string {
	name := ""
	if f.Metadata != nil {
		name = f.Metadata.Name
	}
	return fmt.Sprintf("FH{inum=%d name=%q flags=0x%x}", f.Inum, name, f.Flags)
}

var _ = (fs.FileWriter)((*BangFH)(nil))
var _ = (fs.FileReader)((*BangFH)(nil))

//var _ = (fs.File)

// replaceChunk replaces a chunk in the file with new data
func (f *BangFH) replaceChunk(ctx context.Context, idx int, data []byte) error {
	op := bangutil.GetTracer().Op("replaceChunk", f.Inum, f.Metadata.Name)

	chks := f.Metadata.Chunks
	num_chunks := len(chks)
	if idx >= num_chunks {
		op.Error(fmt.Errorf("chunk index %d out of range (%d chunks)", idx, num_chunks))
		return syscall.EIO
	}

	key := gChunkidgen.NextId()
	err := gKVStore.PutChunk(key, data)
	if err != nil {
		op.Error(err)
		return err
	}

	chks[idx].Hash = key
	chks[idx].Size = uint32(len(data))

	f.Metadata.Chunks = chks
	op.Done()
	return nil
}

// readChunk returns the content of a chunk at index idx
func (f *BangFH) readChunk(ctx context.Context, idx int) ([]byte, error) {
	op := bangutil.GetTracer().Op("BangFH.readChunk", f.Inum, f.Metadata.Name)

	chks := f.Metadata.Chunks
	if idx >= len(chks) || idx < 0 {
		err := fmt.Errorf("chunk index %d out of range (%d chunks)", idx, len(chks))
		op.Error(err)
		return nil, err
	}
	key := chks[idx].Hash
	data, err := gKVStore.Chunk(key)
	if err != nil {
		op.Error(err)
		return nil, err
	}
	op.Done()
	return data, nil
}

// appendChunk appends a new chunk to the file but defers writing metadata
func (f *BangFH) appendChunk(ctx context.Context, data []byte) error {
	op := bangutil.GetTracer().Op("appendChunk", f.Inum, f.Metadata.Name)

	chunkrefs := f.Metadata.Chunks

	key := gChunkidgen.NextId()
	err := gKVStore.PutChunk(key, data)
	if err != nil {
		op.Error(err)
		return err
	}
	// REVISIT: decide if to undo the metadata or resync it if this fails
	chunkrefs = append(chunkrefs, &bangpb.ChunkRef{Hash: key, Size: uint32(len(data))})
	f.Metadata.Chunks = chunkrefs

	op.Done()
	return nil
}

// writeMeta writes the metadata to KV and updates the vclock
func (f *BangFH) writeMeta(ctx context.Context) error {
	op := bangutil.GetTracer().Op("writeMeta", f.Inum, f.Metadata.Name)
	//op.Debugf("Write metadata for inode %d, vclock: %v", f.Inum, f.VClock)

	new_vclock, err := gKVStore.UpdateMetadata(f.Inum, f.Metadata, f.VClock)
	if err != nil {
		op.Error(err)
		// Don't reload the vclock, since our metadata is still stale
		return err
	}

	f.VClock = new_vclock // Our metadata should be in sync with what was written
	//op.Debugf("Metadata updated for inode %d", f.Inum)
	op.Done()
	return nil
}

// resyncMetadata rereads the metadata in case of concurrent modification
func (f *BangFH) resyncMetadata(ctx context.Context) error {
	op := bangutil.GetTracer().Op("resyncMetadata", f.Inum, f.Metadata.Name)
	//op.Debugf("Resync metadata for inode %d", f.Inum)

	metadata, new_vclock, err := gKVStore.Metadata(f.Inum)
	if err != nil {
		op.Error(err)
		return err
	}

	f.VClock = new_vclock
	f.Metadata = metadata
	//op.Debugf("Metadata resynced for inode %d, new vclcok: %v", f.Inum, f.VClock)
	op.Done()
	return nil
}

// writeAt splices data into the file at the given offset, modifying existing
// chunks and appending new ones as needed.
// All chunks except the last are exactly GetChunkSize() bytes, so we use division
// to index directly instead of walking.
func (f *BangFH) writeAt(ctx context.Context, op *bangutil.TraceOp, data []byte, off int64) syscall.Errno {
	chks := f.Metadata.Chunks
	pos := off    // current file position
	data_pos := 0 // how far into data we've consumed

	for data_pos < len(data) {
		chunk_idx := int(pos / int64(GetChunkSize()))
		offset_in_chunk := int(pos % int64(GetChunkSize()))

		if chunk_idx < len(chks) {
			// Overwrite within an existing chunk
			existing, err := f.readChunk(ctx, chunk_idx)
			if err != nil {
				op.Errorf("readChunk[%d]: %v", chunk_idx, err)
				return syscall.EIO
			}
			// Extend the chunk buffer if the write goes past its current size
			// (can happen on the last chunk which may be shorter than GetChunkSize())
			if offset_in_chunk+len(data)-data_pos > len(existing) && len(existing) < int(GetChunkSize()) {
				grown := make([]byte, min(int(GetChunkSize()), offset_in_chunk+len(data)-data_pos))
				copy(grown, existing)
				existing = grown
			}
			n := copy(existing[offset_in_chunk:], data[data_pos:])
			data_pos += n
			pos += int64(n)
			if err := f.replaceChunk(ctx, chunk_idx, existing); err != nil {
				op.Errorf("replaceChunk[%d]: %v", chunk_idx, err)
				return syscall.EIO
			}
		} else {
			// Past the last chunk — append new ones
			remaining := len(data) - data_pos
			n := min(uint32(remaining), GetChunkSize())
			if err := f.appendChunk(ctx, data[data_pos:data_pos+int(n)]); err != nil {
				op.Errorf("appendChunk: %v", err)
				return syscall.EIO
			}
			data_pos += int(n)
			pos += int64(n)
			// appendChunk updates f.Metadata.Chunks, refresh local ref
			chks = f.Metadata.Chunks
		}
	}

	return 0
}

// Write writes to the inode at the given offset and returns the number of bytes written.
// Handles append, overwrite, and write-past-EOF (zero-fill gap).
func (f *BangFH) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	op := bangutil.GetTracer().Op("Write", f.Inum, f.Metadata.Name)
	//op.Debugf("Write %d bytes at offset %d to inode %d", len(data), off, f.Inum)

	// Re-read metadata: Setattr (e.g. O_TRUNC truncate) may have changed it.
	// REVISIT: to save an extra read call we can track filehandles in the BangFile struct.
	if err := f.resyncMetadata(ctx); err != nil {
		op.Error(fmt.Errorf("resyncMetadata: %v", err))
		return 0, syscall.EIO
	}

	file_size := int64(f.Metadata.Size)

	// O_APPEND: force offset to end of file regardless of what the kernel sent
	if f.Flags&syscall.O_APPEND != 0 {
		//op.Debugf("O_APPEND: adjusting offset from %d to %d", off, file_size
		off = file_size

	}

	write_end := off + int64(len(data))

	// If writing past EOF, zero-fill the gap
	if off > file_size {
		gap := make([]byte, off-file_size)
		if errno := f.writeAt(ctx, op, gap, file_size); errno != 0 {
			return 0, errno
		}
		file_size = off
	}

	if errno := f.writeAt(ctx, op, data, off); errno != 0 {
		op.Errno(errno)
		return 0, errno
	}

	// Update file size
	if write_end > file_size {
		f.Metadata.Size = uint64(write_end)
	}

	if err := f.writeMeta(ctx); err != nil {
		op.Error(fmt.Errorf("syncing metadata (chunks and size): %v", err))
		return 0, syscall.EIO
	}

	//op.Debugf("Wrote %d bytes at offset %d (new size: %d)", len(data), off, f.Metadata.Size)
	op.Done()
	return uint32(len(data)), 0
}

// readInto reads len bytes from the given chunk, offset and appends it to data or returns an error
func (f *BangFH) readInto(_ context.Context, chkidx int, off uint64, read_len uint64, out_data *[]byte) error {
	op := bangutil.GetTracer().Op("readInto", f.Inum, "")
	op.Debugf("readInto: chkidx: %d off: %d read_len:%d", chkidx, off, read_len)

	chks := f.Metadata.Chunks
	if chkidx >= len(chks) || chkidx < 0 {
		err := fmt.Errorf("chunk index %d out of range (%d chunks)", chkidx, len(chks))
		op.Error(err)
		return err
	}
	key := chks[chkidx].Hash
	chk_data, err := gKVStore.Chunk(key)
	if err != nil {
		op.Error(err)
		return err
	}

	*out_data = append(*out_data, chk_data[off:off+read_len]...)
	op.Done()
	return nil
}

// Read reads from the file and copies the result to data
func (f *BangFH) Read(ctx context.Context, dest []byte, off_in int64) (fuse.ReadResult, syscall.Errno) {
	op := bangutil.GetTracer().Op("Read", f.Inum, f.Metadata.Name)
	//op.Debugf("Read up to %d bytes at offset %d", len(dest), off)

	//REVISIT: re sync metadata
	//REVISIT: 3 different int types
	off := uint64(off_in)
	file_len := f.Metadata.Size

	if off >= file_len {
		//op.Debugf("offset exceeds file size (%d<%d)", len(dest), off)
		op.Done()
		return fuse.ReadResultData(nil), 0
	}

	out_len := uint64(len(dest))
	out_len = min(out_len, file_len-off)
	out_buf := make([]byte, 0)

	var tot_r_len uint64 = 0
	for tot_r_len < out_len {
		r_off := off + tot_r_len
		r_chkidx := int(r_off / uint64(gChunksize))
		r_chkoffset := r_off % uint64(gChunksize)
		r_len := min(uint64(gChunksize)-r_chkoffset, out_len-tot_r_len)
		if err := f.readInto(ctx, r_chkidx, r_chkoffset, r_len, &out_buf); err != nil {
			op.Errorf("readInto failed: %v", err)
			return fuse.ReadResultData(out_buf), syscall.EIO
		}
		tot_r_len += r_len
	}

	//op.Debugf("Read returning %d bytes", len(buf))
	op.Done()
	return fuse.ReadResultData(out_buf), 0
}
