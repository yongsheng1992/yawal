package log

import (
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	log_v1 "github.com/yongsheng1992/yawal/api/v1"
	"os"
	"path"
	"sync"
)

type Segment struct {
	mu    sync.Mutex
	index *Index
	store *Store

	baseOffset uint64
	nextOffset uint64

	config SegmentConfig
}

func newSegment(dir string, baseOffset uint64, config Config) (*Segment, error) {
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.store", baseOffset)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d.index", baseOffset)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	store, err := newStore(storeFile)
	if err != nil {
		return nil, err
	}
	index, err := newIndex(indexFile, config)
	if err != nil {
		return nil, err
	}
	segment := &Segment{
		store:      store,
		index:      index,
		config:     config.SegmentConfig,
		baseOffset: baseOffset,
		nextOffset: baseOffset,
	}
	if last, err := index.Last(); err == nil {
		segment.nextOffset = baseOffset + last + 1
	}
	return segment, nil
}

func (s *Segment) Append(record *log_v1.Record) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.nextOffset
	record.Offset = cur
	data, err := proto.Marshal(record)

	if s.store.size+uint64(len(data)) > s.config.MaxSegmentSize {
		return 0, ErrExceededMaxSegmentSize
	}

	if err != nil {
		return 0, err
	}
	// todo write payload and index must be atomic.
	// if s.store.write succeed and s.index.write fail, s.store.write must be rollback.
	n, pos, err := s.store.Write(data)
	if err != nil {
		return 0, err
	}
	if int(n) != len(data)+lenWidth {
		return 0, errors.New("write data error")
	}
	if err := s.index.Write(cur-s.baseOffset, pos); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

func (s *Segment) Read(offset uint64) (*log_v1.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, pos, err := s.index.Read(offset - s.baseOffset)
	if err != nil {
		return nil, err
	}

	data, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := new(log_v1.Record)
	if err := proto.Unmarshal(data, record); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *Segment) Close() error {
	if err := s.store.Close(); err != nil {
		return err
	}
	if err := s.index.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(s.index.Name()); err != nil {
		return err
	}
	if err := os.RemoveAll(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *Segment) IndexFileName() string {
	return s.index.Name()
}

func (s *Segment) StoreFileName() string {
	return s.store.Name()
}

func (s *Segment) Size() uint64 {
	return s.store.size
}
