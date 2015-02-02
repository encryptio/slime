package tests

import (
	"git.encryptio.com/slime/lib/meta/store"
)

func clearStore(s store.Store) error {
	_, err := s.RunTx(func(ctx store.Ctx) (interface{}, error) {
		for {
			pairs, err := ctx.Range(nil, nil, 100)
			if err != nil {
				return nil, err
			}

			if len(pairs) == 0 {
				return nil, nil
			}

			for _, pair := range pairs {
				err := ctx.Delete(pair.Key)
				if err != nil {
					return nil, err
				}
			}
		}
	})
	return err
}
