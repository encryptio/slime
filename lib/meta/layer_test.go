package meta

import (
	"crypto/sha256"
	"reflect"
	"testing"
	"time"

	"git.encryptio.com/slime/lib/uuid"

	"git.encryptio.com/kvl"
	"git.encryptio.com/kvl/backend/ram"
)

func TestLayerConfig(t *testing.T) {
	db := ram.New()

	_, err := db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		l, err := Open(ctx)
		if err != nil {
			return nil, err
		}

		value, err := l.GetConfig("a")
		if err != nil {
			t.Errorf("Nonexistent config returned unexpected error %v", err)
			return nil, err
		}

		if string(value) != "" {
			t.Errorf("Nonexistent config returned non-empty string %#v",
				string(value))
		}

		err = l.SetConfig("a", []byte("hello there"))
		if err != nil {
			t.Errorf("Couldn't set config variable: %v", err)
			return nil, err
		}

		value, err = l.GetConfig("a")
		if err != nil {
			t.Errorf("Couldn't get config variable \"a\": %v", err)
			return nil, err
		}

		if string(value) != "hello there" {
			t.Errorf("GetConfig returned %#v, wanted %#v",
				string(value), "hello there")
		}

		return nil, nil
	})
	if err != nil {
		t.Errorf("Couldn't run transaction: %v", err)
	}
}

func TestLayerFileGetSetRemove(t *testing.T) {
	db := ram.New()

	_, err := db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		l, err := Open(ctx)
		if err != nil {
			return nil, err
		}

		fileA := File{
			Path:         "path to a",
			Size:         4,
			SHA256:       sha256.Sum256([]byte("abcd")),
			WriteTime:    uint64(time.Now().Unix()),
			PrefixID:     uuid.Gen4(),
			DataChunks:   2,
			MappingValue: 0,
			Locations:    [][16]byte{uuid.Gen4(), uuid.Gen4()},
		}

		err = l.SetFile(&fileA)
		if err != nil {
			t.Errorf("Couldn't SetFile(%#v): %v", fileA, err)
			return nil, err
		}

		f, err := l.GetFile("path to a")
		if err != nil {
			t.Errorf("Couldn't GetFile(path to a): %v", err)
			return nil, err
		}

		if !reflect.DeepEqual(f, &fileA) {
			t.Errorf("GetFile(path to a) returned %#v, but wanted %#v",
				f, &fileA)
		}

		f, err = l.GetFile("nonexistent")
		if err != nil {
			t.Errorf("Couldn't GetFile(nonexistent): %v", err)
			return nil, err
		}

		if f != nil {
			t.Errorf("GetFile(nonexistent) returned %#v, but wanted nil", f)
		}

		err = l.RemoveFilePath("nonexistent")
		if err != kvl.ErrNotFound {
			t.Errorf("RemoveFilePath(nonexistent) returned %v, but wanted %v",
				err, kvl.ErrNotFound)
			if err != nil {
				return nil, err
			}
		}

		err = l.RemoveFilePath("path to a")
		if err != nil {
			t.Errorf("Couldn't RemoveFilePath(path to a): %v",
				err)
			return nil, err
		}

		f, err = l.GetFile("path to a")
		if err != nil {
			t.Errorf("Couldn't GetFile(path to a): %v", err)
			return nil, err
		}

		if f != nil {
			t.Errorf("GetFile(path to a) returned %#v, but wnated nil", f)
		}

		return nil, nil
	})
	if err != nil {
		t.Errorf("Couldn't run transaction: %v", err)
	}
}

func TestLayerFileGetByLocation(t *testing.T) {
	db := ram.New()

	_, err := db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		l, err := Open(ctx)
		if err != nil {
			return nil, err
		}

		idA := [16]byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		idB := [16]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		idC := [16]byte{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

		fileA := File{
			Path:         "path to a",
			Size:         4,
			SHA256:       sha256.Sum256([]byte("abcd")),
			WriteTime:    uint64(time.Now().Unix()),
			PrefixID:     uuid.Gen4(),
			DataChunks:   2,
			MappingValue: 0,
			Locations:    [][16]byte{idB, idC},
		}
		fileB := File{
			Path:         "path to b",
			Size:         12,
			SHA256:       sha256.Sum256([]byte("goodbye dude")),
			WriteTime:    uint64(time.Now().Unix()),
			PrefixID:     uuid.Gen4(),
			DataChunks:   1,
			MappingValue: 0,
			Locations:    [][16]byte{idA, idB, idC},
		}

		err = l.SetFile(&fileA)
		if err != nil {
			t.Errorf("Couldn't SetFile(%#v): %v", fileA, err)
			return nil, err
		}

		err = l.SetFile(&fileB)
		if err != nil {
			t.Errorf("Couldn't SetFile(%#v): %v", fileB, err)
			return nil, err
		}

		fs, err := l.GetFilesByLocation(idB, 1)
		if err != nil {
			t.Errorf("Couldn't GetFilesByLocation: %v", err)
			return nil, err
		}

		if len(fs) != 1 || !reflect.DeepEqual(fs[0], fileA) {
			t.Errorf("GetFilesByLocation(%v, 1) returned %#v, but wanted %#v",
				uuid.Fmt(idB), fs, []File{fileA})
		}

		fs, err = l.GetFilesByLocation(idA, 3)
		if err != nil {
			t.Errorf("Couldn't GetFilesByLocation: %v", err)
			return nil, err
		}

		if len(fs) != 1 || !reflect.DeepEqual(fs[0], fileB) {
			t.Errorf("GetFilesByLocation(%v, 1) returned %#v, but wanted %#v",
				uuid.Fmt(idA), fs, []File{fileB})
		}

		fs, err = l.GetFilesByLocation(idC, 3)
		if err != nil {
			t.Errorf("Couldn't GetFilesByLocation: %v", err)
			return nil, err
		}

		if len(fs) != 2 ||
			!reflect.DeepEqual(fs[0], fileA) ||
			!reflect.DeepEqual(fs[1], fileB) {

			t.Errorf("GetFilesByLocation(%v, 3) returned %#v, but wanted %#v",
				uuid.Fmt(idC), fs, []File{fileA, fileB})
		}

		return nil, nil
	})
	if err != nil {
		t.Errorf("Couldn't run transaction: %v", err)
	}
}

func TestLayerFileList(t *testing.T) {
	db := ram.New()

	_, err := db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		l, err := Open(ctx)
		if err != nil {
			return nil, err
		}

		for c := 'a'; c <= 'z'; c++ {
			f := File{
				Path:         string(c),
				Size:         1,
				SHA256:       sha256.Sum256([]byte{byte(c)}),
				WriteTime:    uint64(time.Now().Unix()),
				PrefixID:     uuid.Gen4(),
				DataChunks:   1,
				MappingValue: 0,
				Locations:    [][16]byte{uuid.Gen4()},
			}

			err := l.SetFile(&f)
			if err != nil {
				t.Errorf("Couldn't SetFile: %v", err)
				return nil, err
			}
		}

		tests := []struct {
			After string
			Limit int
			Paths []string
		}{
			{"", 2, []string{"a", "b"}},
			{"a", 4, []string{"b", "c", "d", "e"}},
			{"azz", 3, []string{"b", "c", "d"}},
			{"q", 0, []string{"r", "s", "t", "u", "v", "w", "x", "y", "z"}},
			{"w", 6, []string{"x", "y", "z"}},
		}

		for _, test := range tests {
			fs, err := l.ListFiles(test.After, test.Limit)
			if err != nil {
				t.Errorf("Couldn't ListFiles(%#v, %v): %v",
					test.After, test.Limit, err)
				continue
			}

			bad := false
			if len(fs) != len(test.Paths) {
				bad = true
			} else {
				for i, f := range fs {
					if f.Path != test.Paths[i] {
						bad = true
						break
					}
				}
			}

			if bad {
				t.Errorf("ListFiles(%#v, %v) returned %#v, but wanted paths %v",
					test.After, test.Limit, fs, test.Paths)
			}
		}

		return nil, nil
	})
	if err != nil {
		t.Errorf("Couldn't run transaction: %v", err)
	}
}
