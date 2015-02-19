package meta

import (
	"testing"

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
