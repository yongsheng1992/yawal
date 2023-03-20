package yawal

import (
	"github.com/stretchr/testify/require"
	log_v1 "github.com/yongsheng1992/yawal/api/v1"
	"os"
	"testing"
)

var (
	defaultConfig = Config{
		SegmentConfig: SegmentConfig{
			MaxSegmentSize: 1024,
			MaxIndexSize:   1024,
		},
	}
)

func setUp(t *testing.T, dir string) string {
	dir, err := os.MkdirTemp("", "segment_write_and_read")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestSegmentWriteAndRead(t *testing.T) {
	dir := setUp(t, "")
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Fatal(err)
		}
	}(dir)

	seg, err := newSegment(dir, 0, defaultConfig)
	require.NoError(t, err)
	records := []*log_v1.Record{
		{
			Value: []byte("A"),
		},
		{
			Value: []byte("B"),
		},
		{
			Value: []byte("C"),
		},
	}

	for i, record := range records {
		offset, err := seg.Append(record)
		require.NoError(t, err)
		require.Equal(t, uint64(i), offset, "offset must be equal to i")
		r, err := seg.Read(offset)
		require.NoError(t, err)
		require.Equal(t, string(record.Value), string(r.Value), "write and read operates the same data")
	}

	err = seg.Remove()
	require.NoError(t, err)
}

func TestSegmentClose(t *testing.T) {
	dir := setUp(t, "")
	defer func(path string) {
		err := os.RemoveAll(path)
		if err != nil {
			t.Fatal(err)
		}
	}(dir)

	seg, err := newSegment(dir, 0, defaultConfig)
	indexFileName := seg.IndexFileName()
	storeFileName := seg.StoreFileName()

	require.NoError(t, err)
	_, err = os.Stat(indexFileName)
	require.NoError(t, err, "index file must be created")
	_, err = os.Stat(storeFileName)
	require.NoError(t, err, "store file must be create")

	err = seg.Close()
	require.NoError(t, err)

	_, err = seg.Read(uint64(0))
	require.Error(t, err)

	_, err = seg.Append(&log_v1.Record{Value: []byte("hello, world")})
	require.Error(t, err)

	_, err = os.Stat(indexFileName)
	require.False(t, os.IsExist(err), "index file must be deleted")

	_, err = os.Stat(storeFileName)
	require.False(t, os.IsExist(err), "store file must be deleted")
}
