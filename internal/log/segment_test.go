package log

import (
	"io"
	"os"
	"testing"

	api "github.com/cipherzzz/commitlog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "segment_test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	// Create a new segment.
	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset)
	require.False(t, s.IsMaxed())

	// Append to the segment.
	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	// The segment is now maxed.
	require.True(t, s.IsMaxed())

	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)

	// Here we verify that the segment is maxed with bytes storage
	// before we checked for index items max
	c.Segment.MaxIndexBytes = 1024
	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
