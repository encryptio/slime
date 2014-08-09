package multi

import (
	"encoding/json"
	"errors"
	"fmt"
	"git.encryptio.com/slime/lib/store"
	"log"
	"reflect"
	"time"
)

var (
	ErrBadChunkCounts = errors.New("bad chunk counts")
)

type Config struct {
	Version             int64         `json:"version"`
	ScrubFilesPerMinute int64         `json:"scrub_files_per_minute"`
	ScrubTargetDuration time.Duration `json:"scrub_target_duration"`
	ConfigSaveInterval  time.Duration `json:"config_save_interval"`
	ChunksNeed          int           `json:"chunks_need"`
	ChunksTotal         int           `json:"chunks_total"`
}

var DefaultConfig = Config{
	Version:             1,
	ScrubFilesPerMinute: 10,
	ScrubTargetDuration: 7 * 24 * time.Hour,
	ConfigSaveInterval:  30 * time.Minute,
	ChunksNeed:          2,
	ChunksTotal:         3,
}

func (m *Multi) SetConfig(c Config) error {
	if c.ChunksNeed > c.ChunksTotal || c.ChunksNeed < 1 || c.ChunksTotal > 100 {
		return ErrBadChunkCounts
	}

	m.config = c
	return m.saveConfig()
}

func (m *Multi) GetConfig() Config {
	return m.config
}

func (m *Multi) loadConfig() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var configs []Config

	// load configs from all targets
	for _, t := range m.targets {
		data, err := t.GetConfig()
		if err != nil {
			if err != store.ErrItemDoesNotExist {
				log.Printf("Can't get config from %s: %v", t.Name(), err)
			}
			continue
		}

		var config Config
		err = json.Unmarshal(data, &config)
		if err != nil {
			log.Printf("Can't get config from %s: %v", t.Name(), err)
			continue
		}

		configs = append(configs, config)
	}

	if len(configs) > 0 {
		// filter to only the latest configs
		maxVer := configs[0].Version
		for _, config := range configs {
			if config.Version > maxVer {
				maxVer = config.Version
			}
		}

		var saveConfigs []Config
		for _, config := range configs {
			if config.Version == maxVer {
				saveConfigs = append(saveConfigs, config)
			}
		}
		configs = saveConfigs

		// ensure the ones left are identical
		for i := 1; i < len(configs); i++ {
			if !reflect.DeepEqual(configs[i], configs[0]) {
				return fmt.Errorf("Configs of max version differ across Targets. Got %#v and %#v", configs[0], configs[i])
			}
		}

		m.config = configs[0]
	}

	if m.config.Version < 1 {
		log.Printf("Initializing new default configuration")
		m.config = DefaultConfig
	} else {
		log.Printf("Loaded existing configuration version %v", m.config.Version)
	}

	if m.config.ChunksTotal < m.config.ChunksNeed {
		m.config.ChunksTotal = m.config.ChunksNeed
		m.config.Version++
	}

	return nil
}

func (m *Multi) saveConfig() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := json.Marshal(m.config)
	if err != nil {
		return err
	}

	for _, t := range m.targets {
		err := t.SetConfig(data)
		if err != nil {
			log.Printf("Couldn't save config on %v: %v", t.Name(), err)
		}
	}

	return nil
}
