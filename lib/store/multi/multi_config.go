package multi

import (
	"log"
	"strconv"
	"time"

	"git.encryptio.com/slime/lib/meta"
	"git.encryptio.com/slime/lib/uuid"

	"git.encryptio.com/kvl"
)

var (
	loadConfigInterval = time.Minute * 15
)

type BadConfigError string

func (e BadConfigError) Error() string {
	return "bad MultiConfig: " + string(e)
}

type multiConfig struct {
	Need  int
	Total int
}

func checkConfig(config multiConfig) error {
	if config.Need <= 0 {
		return BadConfigError("need is non-positive")
	}
	if config.Total <= 0 {
		return BadConfigError("total is non-positive")
	}
	if config.Need > config.Total {
		return BadConfigError("need is greater than total")
	}
	if config.Total > 100 {
		return BadConfigError("total is too large")
	}
	return nil
}

func (m *Multi) GetRedundancy() (need, total int) {
	m.mu.Lock()
	need = m.config.Need
	total = m.config.Total
	m.mu.Unlock()
	return
}

func (m *Multi) SetRedundancy(need, total int) error {
	m.mu.Lock()
	conf := m.config
	m.mu.Unlock()

	conf.Need = need
	conf.Total = total

	err := checkConfig(conf)
	if err != nil {
		return err
	}

	_, err = m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		err = layer.SetConfig("need", strconv.AppendInt(nil, int64(conf.Need), 10))
		if err != nil {
			return nil, err
		}
		err = layer.SetConfig("total", strconv.AppendInt(nil, int64(conf.Total), 10))
		if err != nil {
			return nil, err
		}

		return nil, nil
	})

	m.mu.Lock()
	m.config = conf
	m.mu.Unlock()

	return nil
}

func (m *Multi) loadUUID() error {
	ret, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		id, err := layer.GetConfig("uuid")
		if err != nil {
			return nil, err
		}

		if len(id) == 0 {
			newID := uuid.Gen4()
			id = newID[:]
			err = layer.SetConfig("uuid", id)
			if err != nil {
				return nil, err
			}
		}

		return id, nil
	})
	if err != nil {
		return err
	}

	copy(m.uuid[:], ret.([]byte))
	return nil
}

func (m *Multi) loadConfig() error {
	_, err := m.db.RunTx(func(ctx kvl.Ctx) (interface{}, error) {
		layer, err := meta.Open(ctx)
		if err != nil {
			return nil, err
		}

		m.mu.Lock()
		conf := m.config
		m.mu.Unlock()

		needBytes, err := layer.GetConfig("need")
		if err != nil {
			return nil, err
		}
		if needBytes == nil {
			needBytes = []byte("3")
		}
		need, err := strconv.ParseInt(string(needBytes), 10, 0)
		if err != nil {
			return nil, err
		}
		conf.Need = int(need)

		totalBytes, err := layer.GetConfig("total")
		if err != nil {
			return nil, err
		}
		if totalBytes == nil {
			totalBytes = []byte("5")
		}
		total, err := strconv.ParseInt(string(totalBytes), 10, 0)
		if err != nil {
			return nil, err
		}
		conf.Total = int(total)

		err = checkConfig(conf)
		if err != nil {
			return nil, err
		}

		m.mu.Lock()
		m.config = conf
		m.mu.Unlock()

		return nil, nil
	})
	return err
}

func (m *Multi) loadConfigLoop(interval time.Duration) {
	for {
		select {
		case <-m.stop:
			return
		case <-time.After(jitterDuration(interval)):
			err := m.loadConfig()
			if err != nil {
				log.Printf("Couldn't load config: %v", err)
			}
		}
	}
}
