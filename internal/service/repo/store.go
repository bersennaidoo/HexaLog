package repo

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	Enc = binary.BigEndian
)

const (
	LenWidth = 8
)

type Store struct {
	File *os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	Size uint64
}

func NewStore(f *os.File) (*Store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	return &Store{
		File: f,
		Size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *Store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.Size

	if err := binary.Write(s.buf, Enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += LenWidth
	s.Size += uint64(w)

	return uint64(w), pos, nil
}

func (s *Store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, LenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, Enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+LenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

func (s *Store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}

func (s *Store) Name() string {
	return s.File.Name()
}
