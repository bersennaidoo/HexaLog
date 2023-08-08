package app

import (
	"fmt"
	"os"
	"path"

	api "github.com/bersennaidoo/HexaLog/cmd/api/v1"
	"github.com/bersennaidoo/HexaLog/config"
	"github.com/bersennaidoo/HexaLog/internal/service/repo"
	"google.golang.org/protobuf/proto"
)

type Segment struct {
	Store                  *repo.Store
	Index                  *repo.Index
	BaseOffset, NextOffset uint64
	Config                 config.Config
}

func NewSegment(dir string, baseOffset uint64, c config.Config) (*Segment, error) {
	s := &Segment{
		BaseOffset: baseOffset,
		Config:     c,
	}

	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)

	if err != nil {
		return nil, err
	}

	if s.Store, err = repo.NewStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	if s.Index, err = repo.NewIndex(indexFile, c); err != nil {
		return nil, err
	}

	if off, _, err := s.Index.Read(-1); err != nil {
		s.NextOffset = baseOffset
	} else {
		s.NextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

func (s *Segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.NextOffset
	record.Offset = cur

	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.Store.Append(p)
	if err != nil {
		return 0, err
	}

	if err = s.Index.Write(
		uint32(s.NextOffset-uint64(s.BaseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	s.NextOffset++

	return cur, nil
}

func (s *Segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.Index.Read(int64(off - s.BaseOffset))
	if err != nil {
		return nil, err
	}

	p, err := s.Store.Read(pos)
	if err != nil {
		return nil, err
	}

	record := &api.Record{}

	err = proto.Unmarshal(p, record)

	return record, err
}

func (s *Segment) IsMaxed() bool {
	return s.Store.Size >= s.Config.Segment.MaxStoreBytes ||
		s.Index.Size >= s.Config.Segment.MaxIndexBytes
}

func (s *Segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.Index.Name()); err != nil {
		return err
	}

	if err := os.Remove(s.Store.Name()); err != nil {
		return err
	}

	return nil
}

func (s *Segment) Close() error {
	if err := s.Index.Close(); err != nil {
		return err
	}
	if err := s.Store.Close(); err != nil {
		return err
	}
	return nil
}

func NearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
