package recio

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
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
	require.NoError(t, err)
	assert.Equal(t, "short", string(string(readBuffer[:n])))
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

func TestBufferedDiskReadTrivial(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "test1.seq")

	wf, err := os.Create(filename)
	require.NoError(t, err)

	w := NewWriter(wf)
	{
		n, err := w.Write([]byte("first message"))
		require.NoError(t, err)
		require.NotZero(t, n)
		n, err = w.Write([]byte("second message"))
		require.NoError(t, err)
		require.NotZero(t, n)
	}
	wf.Close()

	rf, err := os.Open(filename)
	require.NoError(t, err)

	readBuffer := make([]byte, 512)
	r := NewReader(bufio.NewReaderSize(rf, 1024*1024))
	{
		_, err := r.Read(readBuffer)
		require.NoError(t, err)
		_, err = r.Read(readBuffer)
		require.NoError(t, err)
		_, err = r.Read(readBuffer)
		require.ErrorIs(t, err, io.EOF)
	}
}

func TestLotsOfDataToDisk(t *testing.T) {
	numEntries := 1000
	payloadSize := 678

	filename := filepath.Join(t.TempDir(), "lotsofdata.seq")
	wf, err := os.Create(filename)
	require.NoError(t, err)

	w := NewWriter(wf)

	type record struct {
		ID          int    `json:"id"`
		Payload     []byte `json:"payload"`
		PayloadHash []byte `json:"payloadHash"`
	}

	h := sha256.New()

	for i := 0; i < numEntries; i++ {
		rec := record{
			ID:      i,
			Payload: make([]byte, payloadSize),
		}
		n, err := rand.Read(rec.Payload)
		require.NoError(t, err)
		require.NotZero(t, n)
		rec.PayloadHash = h.Sum(rec.Payload)

		jsonBuf, err := json.Marshal(rec)
		require.NoError(t, err)

		n, err = w.Write(jsonBuf)
		require.NoError(t, err)
		require.NotZero(t, n)
	}
	wf.Close()

	readFile, err := os.Open(filename)
	require.NoError(t, err)
	defer readFile.Close()

	readBuffer := make([]byte, 50000)
	reader := bufio.NewReader(NewReader(readFile))
	//reader := NewReader(readFile)
	count := 0
	var rec record
	for {
		n, err := reader.Read(readBuffer)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.NotZero(t, n)

		err = json.Unmarshal(readBuffer[:n], &rec)
		if err != nil {
			log.Printf("count=%d err=%v", count, err)
		}

		require.Equal(t, count, rec.ID)
		require.Equal(t, h.Sum(rec.Payload), rec.PayloadHash)

		count++
	}
	require.Equal(t, numEntries, count)
}

func BenchmarkDiskWrite(b *testing.B) {
	tmpDir := b.TempDir()

	f, err := os.Create(filepath.Join(tmpDir, "test2.seq"))
	require.NoError(b, err)
	defer f.Close()

	w := NewWriter(f)

	buffer := make([]byte, 500)
	n, err := rand.Read(buffer)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.Write(buffer[:n])
	}
}

func BenchmarkDiskRead(b *testing.B) {
	filename := filepath.Join(b.TempDir(), "test3.seq")

	writeFile, err := os.Create(filename)
	require.NoError(b, err)

	w := NewWriter(writeFile)

	buffer := make([]byte, 500)
	n, err := rand.Read(buffer)
	require.NoError(b, err)

	for i := 0; i < b.N; i++ {
		w.Write(buffer[:n])
	}
	writeFile.Close()

	readFile, err := os.Open(filename)
	require.NoError(b, err)

	readBuffer := make([]byte, 500)
	reader := bufio.NewReader(readFile)
	defer readFile.Close()

	b.ResetTimer()
	for {
		n, err := reader.Read(readBuffer)
		if err == io.EOF {
			break
		}
		require.NoError(b, err)
		require.NotZero(b, n)
	}
}
