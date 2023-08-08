package repo

import (
	"io"
	"os"

	"github.com/bersennaidoo/HexaLog/config"
	"github.com/tysonmote/gommap"
)

var (
	OffWidth uint64 = 4
	PosWidth uint64 = 8
	EntWidth        = OffWidth + PosWidth
)

type Index struct {
	File *os.File
	mmap gommap.MMap
	Size uint64
}

func NewIndex(f *os.File, c config.Config) (*Index, error) {
	idx := &Index{
		File: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.Size = uint64(fi.Size())
	if err = os.Truncate(
		f.Name(), int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.File.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

func (i *Index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := i.File.Sync(); err != nil {
		return err
	}

	if err := i.File.Truncate(int64(i.Size)); err != nil {
		return err
	}

	return i.File.Close()
}

func (i *Index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.Size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((i.Size / EntWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * EntWidth
	if i.Size < pos+EntWidth {
		return 0, 0, io.EOF
	}

	out = Enc.Uint32(i.mmap[pos : pos+OffWidth])
	pos = Enc.Uint64(i.mmap[pos+OffWidth : pos+EntWidth])

	return out, pos, nil
}

func (i *Index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.Size+EntWidth {
		return io.EOF
	}

	Enc.PutUint32(i.mmap[i.Size:i.Size+OffWidth], off)
	Enc.PutUint64(i.mmap[i.Size+OffWidth:i.Size+EntWidth], pos)

	i.Size += uint64(EntWidth)

	return nil
}

func (i *Index) Name() string {
	return i.File.Name()
}
