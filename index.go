package yawal

import (
	"github.com/edsrzf/mmap-go"
	"io"
	"os"
)

const (
	offWidth = 8
	posWidth = 8
	entWidth = offWidth + posWidth
)

// FileIndex the binary structure is 8 bits of index and 8 bit of position, so a record index occupied 16 bits.
type Index struct {
	*os.File
	size uint64
	mmap mmap.MMap
}

func newFileIndex(f *os.File, config Config) (*Index, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	if err := os.Truncate(f.Name(), int64(config.SegmentConfig.MaxIndexSize)); err != nil {
		return nil, err
	}
	m, err := mmap.Map(f, mmap.RDWR, 0)
	if err != nil {
		return nil, err
	}
	idx := &Index{
		File: f,
		size: uint64(fi.Size()),
		mmap: m,
	}
	return idx, nil
}

func (idx *Index) Read(off uint64) (n uint64, pos uint64, err error) {
	if idx.size == 0 {
		return 0, 0, io.EOF
	}

	posInIndex := off * entWidth
	if posInIndex > idx.size {
		return 0, 0, io.EOF
	}

	n = endian.Uint64(idx.mmap[posInIndex : posInIndex+offWidth])
	pos = endian.Uint64(idx.mmap[posInIndex+offWidth : posInIndex+entWidth])
	return n, pos, nil
}

// Write writes the offset and its position in the segment.
func (idx *Index) Write(off uint64, pos uint64) error {
	if uint64(len(idx.mmap)) < idx.size+entWidth {
		return io.EOF
	}
	endian.PutUint64(idx.mmap[idx.size:idx.size+offWidth], off)
	endian.PutUint64(idx.mmap[idx.size+offWidth:idx.size+entWidth], pos)
	idx.size += entWidth
	if err := idx.mmap.Flush(); err != nil {
		return err
	}
	return nil
}

func (idx *Index) Close() error {
	if err := idx.mmap.Flush(); err != nil {
		return err
	}
	return idx.mmap.Unmap()
}

func (idx *Index) Size() uint64 {
	return idx.size
}
