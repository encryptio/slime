package multi

import (
	"log"
	"time"

	"git.encryptio.com/slime/lib/meta"

	"git.encryptio.com/kvl"
)

var (
	scrubWALWait = time.Hour * 4
)

func (m *Multi) scrubWALLoop() {
	for {
		select {
		case <-m.stop:
			return
		case <-time.After(jitterDuration(scrubWALWait)):
			err := m.scrubWAL()
			if err != nil {
				log.Printf("Couldn't run scrubWAL: %v", err)
			}
		}
	}
}

func (m *Multi) scrubWAL() error {
	_, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		err = layer.WALClearOld()
		return nil, err
	})
	return err
}
