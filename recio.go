package recio

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type recordWriter struct {
	writer io.Writer
}

type recordReader struct {
	reader io.Reader
}

var (
	ErrTargetBufferTooSmall = errors.New("target buffer is too small to hold message, skipping message")
)

func NewWriter(w io.Writer) io.Writer {
	return &recordWriter{
		writer: w,
	}
}

func (w *recordWriter) Write(p []byte) (int, error) {
	l := uint32(len(p))

	err := binary.Write(w.writer, binary.LittleEndian, l)
	if err != nil {
		return 0, err
	}

	return w.writer.Write(p)
}

func NewReader(r io.Reader) io.Reader {
	return &recordReader{
		reader: r,
	}
}

func (r recordReader) Read(p []byte) (int, error) {
	var length uint32

	err := binary.Read(r.reader, binary.LittleEndian, &length)
	if err != nil {
		return 0, err
	}

	if uint32(len(p)) < length {
		_, err := io.CopyN(io.Discard, r.reader, int64(length))
		if err != nil {
			return 0, fmt.Errorf("error skipping overlong message: %w", err)
		}
		return 0, ErrTargetBufferTooSmall
	}

	return r.reader.Read(p[:length])
}
