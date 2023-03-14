package yawal

import (
	"encoding/binary"
	"os"
	"sync"
)

var (
	endian = binary.BigEndian
)

const (
	lenWidth = 8
)

type Store interface {
	// Read bytes at the pos in the file.
	Read(pos uint64) ([]byte, error)
	// Write data. It returns the bytes of data write, and the position of the data places
	Write(data []byte) (n uint64, pos uint64, err error)
}

type FileStore struct {
	*os.File
	mu   sync.Mutex
	size uint64
}

func newFileStore(f *os.File) (Store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	return &FileStore{
		File: f,
		size: uint64(fi.Size()),
	}, nil
}

func (s *FileStore) Size() (uint64, error) {
	fi, err := os.Stat(s.File.Name())
	if err != nil {
		return 0, err
	}
	return uint64(fi.Size()), nil
}

func (s *FileStore) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	//if err := s.File.Sync(); err != nil {
	//	return nil, err
	//}
	length := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(length, int64(pos)); err != nil {
		return nil, err
	}
	data := make([]byte, endian.Uint64(length))
	if _, err := s.File.ReadAt(data, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return data, nil
}

func (s *FileStore) Write(data []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	buf := make([]byte, lenWidth+len(data))
	endian.PutUint64(buf[0:lenWidth], uint64(len(data)))
	copy(buf[lenWidth:], data)

	pos = s.size
	w, err := s.File.Write(buf)
	if err != nil {
		return 0, 0, err
	}
	s.size += uint64(w)
	//if err := s.buf.Flush(); err != nil {
	//	return 0, 0, err
	//}
	if err := s.File.Sync(); err != nil {
		return 0, 0, err
	}
	return uint64(w), pos, err
}
