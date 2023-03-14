package yawal

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	msg   = "hello, world"
	width = uint64(len(msg)) + lenWidth
	src   = rand.NewSource(time.Now().UnixNano())
)

const (
	letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	// 6 bits to represent a letter index
	letterIdBits = 6
	// All 1-bits as many as letterIdBits
	letterIdMask = 1<<letterIdBits - 1
	letterIdMax  = 63 / letterIdBits
)

func randStr(n int) string {
	sb := strings.Builder{}
	sb.Grow(n)
	// A rand.Int63() generates 63 random bits, enough for letterIdMax letters!
	for i, cache, remain := n-1, src.Int63(), letterIdMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdMax
		}
		if idx := int(cache & letterIdMask); idx < len(letters) {
			sb.WriteByte(letters[idx])
			i--
		}
		cache >>= letterIdBits
		remain--
	}
	return sb.String()
}

func TestStoreWriteAndRead(t *testing.T) {
	f, err := os.CreateTemp("", "test_store_write_and_read")
	require.NoError(t, err)
	fmt.Println(f.Name())
	defer os.RemoveAll(f.Name())
	store, err := newFileStore(f)
	require.NoError(t, err)
	testWrite(t, store)
	testRead(t, store)
}

func testWrite(t *testing.T, s Store) {
	t.Helper()
	for i := 0; i < 4; i++ {
		n, pos, err := s.Write([]byte(msg))
		require.NoError(t, err)
		require.Equal(t, pos+n, width*uint64(i+1))
	}
}

func testRead(t *testing.T, s Store) {
	t.Helper()
	for i := 0; i < 4; i++ {
		pos := width * uint64(i)
		data, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, string(data), msg)
	}
}

func BenchmarkFileStore_Write(b *testing.B) {
	b.StopTimer()
	msg := []byte(randStr(1024))
	f, err := os.CreateTemp("", "test_store_write_and_read")
	require.NoError(b, err)
	defer os.RemoveAll(f.Name())
	store, err := newFileStore(f)
	require.NoError(b, err)
	//b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = store.Write(msg)
	}
}
