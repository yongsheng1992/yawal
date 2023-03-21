package yawal

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestLogWriteAndRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "log-test")
	require.NoError(t, err)
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Fatal(err)
		}
	}(dir)

	log, err := NewLog(dir, defaultConfig)
	require.NoError(t, err)
	defer func(log *Log) {
		err := log.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(log)

	require.Equal(t, len(log.segments), 1)

	msgs := [][]byte{[]byte("hello"), []byte("world")}

	for i, msg := range msgs {
		offset, err := log.Append(msg)
		require.NoError(t, err)
		require.Equal(t, offset, uint64(i))
		data, err := log.Read(uint64(i))
		require.NoError(t, err)
		require.Equal(t, string(data), string(msg))
	}
}

func TestSegmentMaxSize(t *testing.T) {
	dir, err := os.MkdirTemp("", "log-test")
	require.NoError(t, err)
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Fatal(err)
		}
	}(dir)

	config := Config{
		SegmentConfig: SegmentConfig{
			MaxSegmentSize: 128,
			MaxIndexSize:   1024,
		},
	}

	log, err := NewLog(dir, config)
	require.NoError(t, err)
	defer func(log *Log) {
		err := log.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(log)

	msgA := randStr(65)
	msgB := randStr(65)

	offsetA, err := log.Append([]byte(msgA))
	require.NoError(t, err)
	require.Equal(t, offsetA, uint64(0))
	offsetB, err := log.Append([]byte(msgB))
	require.NoError(t, err)
	require.Equal(t, offsetB, uint64(1))

	a, err := log.Read(offsetA)
	require.NoError(t, err)
	require.Equal(t, string(a), msgA)
	b, err := log.Read(offsetB)
	require.NoError(t, err)
	require.Equal(t, string(b), msgB)

	require.Equal(t, len(log.segments), 2)

}

func TestRestore(t *testing.T) {
	config := Config{
		SegmentConfig: SegmentConfig{
			MaxSegmentSize: 128,
			MaxIndexSize:   1024,
		},
	}
	dir, err := os.MkdirTemp("", "log-test")

	require.NoError(t, err)
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Fatal(err)
		}
	}(dir)

	log, err := NewLog(dir, config)

	require.NoError(t, err)

	msgs := make([][]byte, 0)
	for i := 0; i < 1024; i++ {
		msgs = append(msgs, []byte(randStr(52)))
	}

	for i, msg := range msgs {
		offset, err := log.Append(msg)
		require.NoError(t, err)
		require.Equal(t, offset, uint64(i))
	}

	require.Equal(t, 512, len(log.segments))

	for i := 0; i < 512; i++ {
		seg := log.segments[i]
		require.Equal(t, seg.baseOffset, uint64(i)*2)
		require.Equal(t, seg.nextOffset, uint64(i+1)*2)
	}

	if err := log.Close(); err != nil {
		require.NoError(t, err)
	}

	logN, err := NewLog(dir, config)

	require.NoError(t, err)
	defer func(log *Log) {
		err := log.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(logN)

	for i := 0; i < 512; i++ {
		seg := logN.segments[i]
		require.Equal(t, seg.baseOffset, uint64(i)*2)
		require.Equal(t, seg.nextOffset, uint64(i+1)*2)
	}
	for i := 0; i < 1024; i++ {
		b, err := logN.Read(uint64(i))
		require.NoError(t, err)
		require.Equal(t, string(b), string(msgs[i]))
	}
}

func TestCompact(t *testing.T) {
	config := Config{
		SegmentConfig: SegmentConfig{
			MaxSegmentSize: 128,
			MaxIndexSize:   1024,
		},
	}
	dir, err := os.MkdirTemp("", "log-test")

	require.NoError(t, err)
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Fatal(err)
		}
	}(dir)

	log, err := NewLog(dir, config)

	require.NoError(t, err)
	defer func(log *Log) {
		err := log.Close()
		if err != nil {
			t.Fatal(err)
		}
	}(log)

	msgs := make([][]byte, 0)
	for i := 0; i < 1024; i++ {
		msgs = append(msgs, []byte(randStr(52)))
	}

	for i, msg := range msgs {
		offset, err := log.Append(msg)
		require.NoError(t, err)
		require.Equal(t, offset, uint64(i))
	}

	// now the max offset is 1023, so delete the previous 512 offsets
	err = log.Compact(512)
	require.NoError(t, err)

	_, err = log.Read(511)
	require.Equal(t, err, ErrIllegalOffsetRange)
	b, err := log.Read(512)
	require.Equal(t, string(b), string(msgs[512]))
}
