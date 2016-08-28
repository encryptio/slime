package multi

import (
	"log"
	"time"
)

func (m *Multi) asyncDeletionLoop() error {
	for {
		select {
		case <-m.tomb.Dying():
			// NB: Even if we exit early without deleting everything, that's
			// okay; the scrubber will delete those files later.
			return nil
		case file := <-m.asyncDeletions:
			err := m.deleteChunks(file)
			if err != nil {
				log.Printf("Couldn't run delete chunks: %v", err)
			}
		case m.asyncDeletionsReading <- struct{}{}:
		}
	}
}

func (m *Multi) waitAsyncDeletionDone() {
	for len(m.asyncDeletions) > 0 {
		time.Sleep(time.Millisecond)
	}
	<-m.asyncDeletionsReading
}
