package meta

import (
	"errors"
	"log"
	"time"

	"git.encryptio.com/kvl"
	"git.encryptio.com/kvl/index"
	"git.encryptio.com/kvl/keys"
	"git.encryptio.com/kvl/tuple"

	"git.encryptio.com/slime/lib/uuid"
)

var (
	ErrBadArgument = errors.New("bad argument")

	walExpireAge       = time.Hour * 24 * 7
	walUnsafeOldAge    = time.Hour * 24 * 90
	walUnsafeFutureAge = time.Minute
)

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

func (l *Layer) GetConfig(key string) ([]byte, error) {
	p, err := l.inner.Get(tuple.MustAppend(nil, "config", key))
	if err != nil && err != kvl.ErrNotFound {
		return nil, err
	}
	return p.Value, nil
}

func (l *Layer) SetConfig(key string, data []byte) error {
	return l.inner.Set(kvl.Pair{tuple.MustAppend(nil, "config", key), data})
}

func (l *Layer) WALMark(id [16]byte) error {
	return l.inner.Set(kvl.Pair{
		tuple.MustAppend(nil, "wal", id),
		tuple.MustAppend(nil, time.Now().Unix()),
	})
}

func (l *Layer) WALClear(id [16]byte) error {
	return l.inner.Delete(tuple.MustAppend(nil, "wal", id))
}

func (l *Layer) WALCheck(id [16]byte) (bool, error) {
	_, err := l.inner.Get(tuple.MustAppend(nil, "wal", id))
	if err != nil {
		if err == kvl.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (l *Layer) WALClearOld() error {
	var rang kvl.RangeQuery
	rang.Low, rang.High = keys.PrefixRange(tuple.MustAppend(nil, "wal"))
	rang.Limit = 100

	for {
		ps, err := l.inner.Range(rang)
		if err != nil {
			return err
		}

		now := time.Now().Unix()

		for _, p := range ps {
			var t int64
			var typ string
			var id [16]byte
			err = tuple.UnpackInto(p.Key, &typ, &id)
			if err != nil {
				return err
			}
			err = tuple.UnpackInto(p.Value, &t)
			if err != nil {
				return err
			}

			age := time.Duration(now-t) * time.Second

			if age < -walUnsafeFutureAge {
				log.Printf("WARNING: WAL entry %v is %v in the future! Skipping it.",
					uuid.Fmt(id), age)
			} else if age > walUnsafeOldAge {
				log.Printf("WARNING: WAL entry %v is %v in the past! Skipping it.",
					uuid.Fmt(id), age)
			} else if age > walExpireAge {
				log.Printf("Removing expired WAL entry %v (%v old)",
					uuid.Fmt(id), age)
				err := l.WALClear(id)
				if err != nil {
					return err
				}
			}
		}

		if len(ps) < rang.Limit {
			break
		}

		rang.Low = ps[len(ps)-1].Key
	}

	return nil
}

func (l *Layer) GetFile(path string) (*File, error) {
	pair, err := l.inner.Get(fileKey(path))
	if err != nil {
		if err == kvl.ErrNotFound {
			return nil, nil
		}
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
	return l.inner.Delete(fileKey(path))
}

func (l *Layer) GetFilesByLocation(id [16]byte, count int) ([]File, error) {
	var rang kvl.RangeQuery
	rang.Low, rang.High = keys.PrefixRange(tuple.MustAppend(nil,
		"file", "location", id))
	rang.Limit = count

	ps, err := l.index.Range(rang)
	if err != nil {
		return nil, err
	}

	fs := make([]File, 0, len(ps))

	for _, p := range ps {
		var typ, detail, path string
		var loc [16]byte
		err := tuple.UnpackInto(p.Key, &typ, &detail, &loc, &path)
		if err != nil {
			return nil, err
		}

		f, err := l.GetFile(path)
		if err != nil {
			return nil, err
		}

		if f != nil {
			fs = append(fs, *f)
		}
	}

	return fs, nil
}

func (l *Layer) GetLocationContents(id [16]byte, after string, count int) ([]string, error) {
	var rang kvl.RangeQuery
	rang.Low = keys.LexNext(tuple.MustAppend(nil, "locationlist", id, after))
	rang.High = keys.PrefixNext(tuple.MustAppend(nil, "locationlist", id))
	if count > 0 {
		rang.Limit = count + 1
	}

	ps, err := l.index.Range(rang)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(ps))

	for _, p := range ps {
		var typ, name string
		var loc [16]byte
		err := tuple.UnpackInto(p.Key, &typ, &loc, &name)
		if err != nil {
			return nil, err
		}

		if name > after {
			names = append(names, name)
			if count > 0 && len(names) >= count {
				break
			}
		}
	}

	return names, nil
}

func (l *Layer) LocationShouldHave(id [16]byte, name string) (bool, error) {
	_, err := l.index.Get(tuple.MustAppend(nil, "locationlist", id, name))
	if err != nil {
		if err == kvl.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (l *Layer) PathForPrefixID(id [16]byte) (string, error) {
	p, err := l.index.Get(tuple.MustAppend(nil, "file", "prefix", id))
	if err != nil {
		return "", err
	}
	return string(p.Value), nil
}

func (l *Layer) ListFiles(after string, limit int) ([]File, error) {
	if limit < 0 {
		return nil, ErrBadArgument
	}

	var query kvl.RangeQuery
	query.Low = fileKey(after)
	query.High = keys.PrefixNext(tuple.MustAppend(nil, "file"))
	if limit > 0 {
		query.Limit = limit + 1
	}
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
			if limit > 0 && len(files) == limit {
				break
			}
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

func Reindex(db kvl.DB) error {
	progress := make(chan index.ReindexStats)
	defer close(progress)
	go func() {
		for stats := range progress {
			log.Printf("Reindexing... so far: %v", stats)
		}
	}()

	_, err := index.Reindex(db, indexFn, progress)
	if err != nil {
		log.Printf("Couldn't reindex: %v", err)
	}

	return err
}
