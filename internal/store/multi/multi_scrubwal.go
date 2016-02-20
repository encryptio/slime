package multi

import (
	"log"
	"time"

	"github.com/encryptio/slime/internal/meta"

	"github.com/encryptio/kvl"
)

var (
	scrubWALWait = time.Hour * 4
)

func (m *Multi) scrubWALLoop() error {
	for {
		select {
		case <-m.tomb.Dying():
			return nil
		case <-time.After(jitterDuration(scrubWALWait)):
			err := m.scrubWAL()
			if err != nil {
				log.Printf("Couldn't run scrubWAL: %v", err)
			}
		}
	}
}

func (m *Multi) scrubWAL() error {
	return m.db.RunTx(func(ctx kvl.Ctx) error {
		layer, err := meta.Open(ctx)
		if err != nil {
			return err
		}

		return layer.WALClearOld()
	})
}
