package meta

import (
	"errors"

	"git.encryptio.com/slime/lib/meta/store"
)

var (
	ErrNotFound    = errors.New("file not found")
	ErrBadArgument = errors.New("bad argument")
)

const MaxListLimit = 10001

type Layer struct {
	St store.Store
}

func (l *Layer) GetFile(path string) (*File, error) {
	ret, err := l.St.RunTx(func(ctx store.Ctx) (interface{}, error) {
		var ret File

		pair, err := ctx.Get(fileKey(path))
		if err != nil {
			return nil, err
		}

		err = ret.fromPair(pair)
		if err != nil {
			return nil, err
		}

		return &ret, nil
	})
	file, _ := ret.(*File)
	return file, err
}

// returns old File in database, if any
func (l *Layer) SetFile(f *File) (*File, error) {
	ret, err := l.St.RunTx(func(ctx store.Ctx) (interface{}, error) {
		var old *File

		pair, err := ctx.Get(fileKey(f.Path))
		if err == store.ErrNotFound {
			// do nothing
		} else if err != nil {
			return nil, err
		} else { // err == nil
			old = new(File)
			err := old.fromPair(pair)
			if err != nil {
				return nil, err
			}
		}

		pair = f.toPair()

		err = ctx.Set(pair)
		if err != nil {
			return nil, err
		}

		return old, nil
	})
	old, _ := ret.(*File)
	return old, err
}

func (l *Layer) RemoveFilePath(path string) error {
	_, err := l.St.RunTx(func(ctx store.Ctx) (interface{}, error) {
		err := ctx.Delete(fileKey(path))
		// ErrNotFound will abort the transaction, but that's okay in this case
		return nil, err
	})
	if err == store.ErrNotFound {
		return ErrNotFound
	}
	return err
}

func (l *Layer) List(prefix string, limit int) ([]File, error) {
	if limit < 1 || limit > MaxListLimit {
		return nil, ErrBadArgument
	}
	ret, err := l.St.RunTx(func(ctx store.Ctx) (interface{}, error) {
		low, high := store.PrefixRange(fileKey(prefix))
		pairs, err := ctx.Range(low, high, limit)
		if err != nil {
			return nil, err
		}

		files := make([]File, len(pairs))
		for i, pair := range pairs {
			err := files[i].fromPair(pair)
			if err != nil {
				return nil, err
			}
		}

		return files, nil
	})
	files, _ := ret.([]File)
	return files, err
}
