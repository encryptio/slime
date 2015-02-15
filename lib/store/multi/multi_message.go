package multi

import (
	"fmt"
	"log"
)

func (m *Multi) SaveMessage(msg string) error {
	// TODO: save to database
	log.Printf("message: %v", msg)
	return nil
}

func (m *Multi) SaveMessagef(format string, a ...interface{}) error {
	return m.SaveMessage(fmt.Sprintf(format, a...))
}
