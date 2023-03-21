package log

import (
	log_v1 "github.com/yongsheng1992/yawal/api/v1"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Log struct {
	mu            sync.Mutex
	segments      []*Segment
	activeSegment *Segment
	Config        Config

	Dir string
}

func NewLog(dir string, config Config) (*Log, error) {
	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	baseOffsets := make([]uint64, 0)
	for _, entry := range dirEntries {
		offsetStr := strings.Trim(entry.Name(), path.Ext(entry.Name()))
		offset, err := strconv.ParseUint(offsetStr, 10, 0)
		if err != nil {
			return nil, err
		}
		baseOffsets = append(baseOffsets, offset)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	log := &Log{
		segments: make([]*Segment, 0),
		Config:   config,
		Dir:      dir,
	}
	for i := 0; i < len(baseOffsets); i++ {
		baseOffset := baseOffsets[i]
		seg, err := newSegment(dir, baseOffset, config)
		if err != nil {
			return nil, err
		}
		log.segments = append(log.segments, seg)
		i++
	}

	n := len(log.segments)
	if n == 0 {
		if err := log.newSegment(uint64(0)); err != nil {
			return nil, err
		}
	} else {
		log.activeSegment = log.segments[n-1]
	}
	return log, nil
}

func (l *Log) Append(data []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	record := &log_v1.Record{
		Value: data,
	}

	if l.activeSegment.Size()+uint64(len(data)) > l.Config.SegmentConfig.MaxSegmentSize {
		if err := l.newSegment(l.activeSegment.nextOffset); err != nil {
			return 0, err
		}
	}

	offset, err := l.activeSegment.Append(record)

	if err != err {
		return 0, err
	}

	return offset, nil
}

func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// todo handle read offset less than 0
	if offset >= l.activeSegment.nextOffset {
		return nil, ErrIllegalOffsetRange
	}

	target := l.activeSegment
	var i int
	if offset < l.activeSegment.baseOffset {
		for i = 0; i < len(l.segments); i++ {
			if offset >= l.segments[i].baseOffset && offset < l.segments[i].nextOffset {
				break
			}
		}
		if i == len(l.segments) {
			return nil, ErrIllegalOffsetRange
		}
		target = l.segments[i]
	}
	record, err := target.Read(offset)

	if err != nil {
		return nil, err
	}
	return record.Value, nil
}

// newSegment create a new segment. This method is not concurrent safety, so the caller must hold the lock.
func (l *Log) newSegment(baseOffset uint64) error {
	seg, err := newSegment(l.Dir, baseOffset, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, seg)
	l.activeSegment = seg
	return nil
}

func (l *Log) Close() error {
	for _, seg := range l.segments {
		err := seg.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Compact(offset uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if offset > l.activeSegment.nextOffset {
		return ErrIllegalOffsetRange
	}

	var i int
	for i = 0; i < len(l.segments); i++ {
		seg := l.segments[i]
		if seg.nextOffset < offset {
			if err := seg.Remove(); err != nil {
				return err
			}
		} else {
			break
		}
	}
	l.segments = l.segments[i+1:]
	return nil
}
