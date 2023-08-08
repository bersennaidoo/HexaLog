package app

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/bersennaidoo/HexaLog/cmd/api/v1"
	"github.com/bersennaidoo/HexaLog/config"
	"github.com/bersennaidoo/HexaLog/internal/service/repo"
)

type Log struct {
	mu sync.RWMutex

	Dir    string
	Config config.Config

	ActiveSegment *Segment
	Segments      []*Segment
}

func NewLog(dir string, c config.Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

func (l *Log) setup() error {
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.NewSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffset contains dup for index and store so we skip
		// the dup
		i++
	}
	if l.Segments == nil {
		if err = l.NewSegment(
			l.Config.Segment.InitialOffset,
		); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.ActiveSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.ActiveSegment.IsMaxed() {
		err = l.NewSegment(off + 1)
	}
	return off, err
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *Segment
	for _, segment := range l.Segments {
		if segment.BaseOffset <= off && off < segment.NextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.NextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}
	return s.Read(off)
}

func (l *Log) NewSegment(off uint64) error {
	s, err := NewSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.Segments = append(l.Segments, s)
	l.ActiveSegment = s
	return nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.Segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.Segments[0].BaseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.Segments[len(l.Segments)-1].NextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*Segment
	for _, s := range l.Segments {
		if s.NextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.Segments = segments
	return nil
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.Segments))
	for i, segment := range l.Segments {
		readers[i] = &OriginReader{segment.Store, 0}
	}
	return io.MultiReader(readers...)
}

type OriginReader struct {
	*repo.Store
	off int64
}

func (o *OriginReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}
