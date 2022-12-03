package recio

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimple(t *testing.T) {
	writer := bytes.NewBuffer([]byte{})

	w := NewWriter(writer)
	for i := 0; i < 10; i++ {
		n, err := w.Write([]byte(fmt.Sprintf("this is test string %d", i)))
		require.NoError(t, err)
		require.Greater(t, n, 10)
	}

	data := writer.Bytes()

	readBuffer := make([]byte, 500)

	r := NewReader(bytes.NewReader(data))
	for {
		n, err := r.Read(readBuffer)
		if err == io.EOF {
			break
		} else {
			require.NoError(t, err)
		}
		require.NotZero(t, n)
	}
}

func TestVariableSizes(t *testing.T) {

	writer := bytes.NewBuffer([]byte{})
	w := NewWriter(writer)

	maxMessageSize := 1024 * 1024

	// fill buffer with junk
	tmpBuffer := make([]byte, maxMessageSize)
	n, err := rand.Read(tmpBuffer)
	require.NoError(t, err)
	require.Equal(t, n, maxMessageSize)

	numMessages := 20
	sizes := make([]int, numMessages)

	for i := 0; i < numMessages; i++ {
		sizes[i] = rand.Intn(maxMessageSize) // store the sizes so we can compare later
		n, err := w.Write(tmpBuffer[:sizes[i]])
		require.NoError(t, err)
		require.Equal(t, sizes[i], n) // make sure the whole message got written
	}

	r := NewReader(bytes.NewReader(writer.Bytes()))
	for i := 0; ; i++ {
		n, err := r.Read(tmpBuffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}

		require.Equal(t, sizes[i], n) // compare to stored sizes
	}
}

// Make sure we can skip over buffers that are too large for the read buffer we use.
func TestTargetBufferTooSmall(t *testing.T) {
	writer := bytes.NewBuffer([]byte{})
	w := NewWriter(writer)

	// this message is 21 bytes
	w.Write([]byte("this is test string 0"))
	w.Write([]byte("short"))

	// make the readbuffer too small
	readBuffer := make([]byte, 10)
	r := NewReader(bytes.NewReader(writer.Bytes()))

	// first message should trigger error
	_, err := r.Read(readBuffer)
	require.ErrorIs(t, err, ErrTargetBufferTooSmall)

	// second message should read fine
	n, err := r.Read(readBuffer)
	require.NoError(t, err)
	assert.Equal(t, "short", string(readBuffer[:n]))
}

func BenchmarkWriter(b *testing.B) {
	w := NewWriter(io.Discard)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := w.Write([]byte("this is a test"))
		require.NoError(b, err)
	}
}

func BenchmarkReader(b *testing.B) {
	writer := bytes.NewBuffer([]byte{})

	w := NewWriter(writer)
	for i := 0; i < b.N; i++ {
		n, err := w.Write([]byte(fmt.Sprintf("this is test string %d", i)))
		require.NoError(b, err)
		require.Greater(b, n, 10)
	}

	readBuffer := make([]byte, 500)
	r := NewReader(bytes.NewReader(writer.Bytes()))

	b.ResetTimer()
	for {
		_, err := r.Read(readBuffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			require.NoError(b, err)

		}
	}
}
