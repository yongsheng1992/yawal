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

}
