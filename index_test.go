package yawal

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestIndexWriteAndRead(t *testing.T) {
	f, err := os.CreateTemp("", "test_index_write_and_read")
	require.NoError(t, err)
	defer os.RemoveAll(f.Name())

	config := Config{
		SegmentConfig: SegmentConfig{
			MaxIndexSize: 1024 * 4,
		},
	}
	idx, err := newIndex(f, config)
	require.NoError(t, err)
	defer idx.Close()

	type args struct {
		off uint64
		pos uint64
	}

	argsList := []args{
		{
			off: uint64(0),
			pos: uint64(0),
		},
		{
			off: uint64(1),
			pos: uint64(2),
		},
	}

	for i, args := range argsList {
		err = idx.Write(args.off, args.pos)
		require.NoError(t, err)
		require.Equal(t, uint64(i+1)*entWidth, idx.Size())
		n, pos, err := idx.Read(args.off)
		require.NoError(t, err)
		require.Equal(t, pos, args.pos)
		require.Equal(t, n, args.off)
	}
}
