package meta

import (
	"errors"
	"log"

	"git.encryptio.com/kvl"
	"git.encryptio.com/kvl/index"
	"git.encryptio.com/kvl/keys"
	"git.encryptio.com/kvl/tuple"
)

var (
	ErrNotFound    = errors.New("file not found")
	ErrBadArgument = errors.New("bad argument")

	configKey = tuple.MustAppend(nil, "config")
)

const MaxListLimit = 10001

type Layer struct {
	ctx   kvl.Ctx
	inner kvl.Ctx
	index *index.Index
}

func Open(ctx kvl.Ctx) (*Layer, error) {
	inner, idx, err := index.Open(ctx, indexFn)
	if err != nil {
		return nil, err
	}

	return &Layer{
		ctx:   ctx,
		inner: inner,
		index: idx,
	}, nil
}

func (l *Layer) GetConfig() ([]byte, error) {
	p, err := l.inner.Get(configKey)
	if err != nil && err != kvl.ErrNotFound {
		return nil, err
	}
	return p.Value, nil
}

func (l *Layer) SetConfig(conf []byte) error {
	return l.inner.Set(kvl.Pair{configKey, conf})
}

func (l *Layer) GetFile(path string) (*File, error) {
	pair, err := l.inner.Get(fileKey(path))
	if err != nil {
		return nil, err
	}

	var f File
	err = f.fromPair(pair)
	if err != nil {
		return nil, err
	}

	return &f, nil
}

func (l *Layer) SetFile(f *File) error {
	return l.inner.Set(f.toPair())
}

func (l *Layer) RemoveFilePath(path string) error {
	err := l.inner.Delete(fileKey(path))
	if err == kvl.ErrNotFound {
		return ErrNotFound
	}
	return err
}

func (l *Layer) ListFiles(after string, limit int) ([]File, error) {
	if limit < 1 || limit > MaxListLimit {
		return nil, ErrBadArgument
	}

	var query kvl.RangeQuery
	query.Low = fileKey(after)
	query.Limit = limit
	pairs, err := l.inner.Range(query)
	if err != nil {
		return nil, err
	}

	files := make([]File, 0, len(pairs))
	for _, pair := range pairs {
		var f File
		err := f.fromPair(pair)
		if err != nil {
			return nil, err
		}

		if f.Path > after {
			files = append(files, f)
		}
	}

	return files, nil
}

func (l *Layer) AllLocations() ([]Location, error) {
	var query kvl.RangeQuery
	key, _ := tuple.Append(nil, "location")
	query.Low, query.High = keys.PrefixRange(key)

	pairs, err := l.inner.Range(query)
	if err != nil {
		return nil, err
	}

	locations := make([]Location, len(pairs))
	for i, pair := range pairs {
		err := locations[i].fromPair(pair)
		if err != nil {
			return nil, err
		}
	}

	return locations, nil
}

func (l *Layer) GetLocation(uuid [16]byte) (*Location, error) {
	loc := Location{UUID: uuid}
	q := loc.toPair()
	pair, err := l.inner.Get(q.Key)
	if err != nil {
		if err == kvl.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	err = loc.fromPair(pair)
	if err != nil {
		return nil, err
	}
	return &loc, nil
}

func (l *Layer) DeleteLocation(loc Location) error {
	pair := loc.toPair()
	return l.inner.Delete(pair.Key)
}

func (l *Layer) SetLocation(loc Location) error {
	pair := loc.toPair()
	return l.inner.Set(pair)
}

func Reindex(db kvl.DB, deleteIndicies bool) error {
	progress := make(chan index.ReindexStats)
	defer close(progress)
	go func() {
		for stats := range progress {
			log.Printf("Reindexing... so far: %v", stats)
		}
	}()

	options := uint64(0)
	if deleteIndicies {
		options |= index.REINDEX_DELETE
	}

	_, err := index.Reindex(db, indexFn, options, progress)
	if err != nil {
		log.Printf("Couldn't reindex: %v", err)
	}

	return err
}
