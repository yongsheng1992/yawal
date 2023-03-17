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

type Store struct {
	*os.File
	mu   sync.Mutex
	size uint64
}

func newFileStore(f *os.File) (*Store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	return &Store{
		File: f,
		size: uint64(fi.Size()),
	}, nil
}

func (s *Store) Size() (uint64, error) {
	fi, err := os.Stat(s.File.Name())
	if err != nil {
		return 0, err
	}
	return uint64(fi.Size()), nil
}

func (s *Store) Read(pos uint64) ([]byte, error) {
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

func (s *Store) Write(data []byte) (n uint64, pos uint64, err error) {
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
	// todo Use fdatasync instead of fsync. Cause MacOS does not support fdatasync immediately, so there is no
	// need to support fdatasync at early version.
	if err := s.File.Sync(); err != nil {
		return 0, 0, err
	}

	return uint64(w), pos, err
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.File.Close(); err != nil {
		return err
	}
	return nil
}
