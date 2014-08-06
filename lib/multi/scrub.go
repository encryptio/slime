package multi

import (
	"git.encryptio.com/slime/lib/chunk"
	"git.encryptio.com/slime/lib/store"
	"log"
	"math/rand"
	"time"
)

type ScrubStats struct {
	Running           bool          `json:"running"`
	LastDuration      time.Duration `json:"last_duration"`
	LastFixedErrors   int64         `json:"last_fixed_errors"`
	LastUnfixedErrors int64         `json:"last_unfixed_errors"`
	LastOkay          int64         `json:"last_okay"`
	LastStartedAt     time.Time     `json:"last_started_at"`

	StartedAt     time.Time `json:"started_at,omitempty"`
	FixedErrors   int64     `json:"fixed_errors"`
	UnfixedErrors int64     `json:"unfixed_errors"`
	Okay          int64     `json:"okay"`
}

func (m *Multi) GetScrubStats() ScrubStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.scrubStats
}

func (m *Multi) scrubLoop() {
	for {
		start := time.Now()
		m.scrub()
		end := time.Now()

		duration := end.Sub(start)
		m.rotateScrubStats(duration)
		m.updateScrubRate(duration)
		m.scrubSleep(func() {})
	}
}

func (m *Multi) updateScrubRate(d time.Duration) {
	m.mu.Lock()

	shouldSave := false
	if d > m.config.ScrubTargetDuration+m.config.ScrubTargetDuration/4 && m.config.ScrubFilesPerMinute < 1000 {
		log.Printf("Last scrub duration was too long (%v, want %v), increasing scrub files per minute", d, m.config.ScrubTargetDuration)
		m.config.ScrubFilesPerMinute += m.config.ScrubFilesPerMinute >> 3
		m.config.Version++
		shouldSave = true
	} else if d < m.config.ScrubTargetDuration-m.config.ScrubTargetDuration/4 && m.config.ScrubFilesPerMinute > 10 {
		log.Printf("Last scrub duration was too short (%v, want %v), decreasing scrub files per minute", d, m.config.ScrubTargetDuration)
		m.config.ScrubFilesPerMinute -= m.config.ScrubFilesPerMinute >> 3
		m.config.Version++
		shouldSave = true
	}

	m.mu.Unlock()

	if shouldSave {
		m.saveConfig()
	}
}

func (m *Multi) scrub() {
	m.setScrubRunning(true)
	defer m.setScrubRunning(false)
	m.scrubRec("/")
	m.scrubSleep(func() {})
}

func (m *Multi) scrubRec(path string) {
	l, err := m.List(path)
	if err != nil {
		log.Printf("Couldn't scrub directory %v: %v", path, err)
		m.incrementScrubErrors(false)
		return
	}

	// Fischer-Yates shuffle
	for i := len(l) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		l[i], l[j] = l[j], l[i]
	}

	for _, fi := range l {
		m.scrubSleep(func() {
			if fi.IsDir {
				m.scrubRec(fi.Name + "/")
			} else {
				m.scrubFile(fi.Name)
			}
		})
	}
}

func (m *Multi) scrubSleep(inner func()) {
	m.mu.Lock()
	fpm := m.config.ScrubFilesPerMinute
	m.mu.Unlock()

	until := time.Now().Add(time.Minute / time.Duration(fpm))

	inner()

	d := until.Sub(time.Now())
	if d > 0 {
		time.Sleep(d)
	}
}

func (m *Multi) setScrubRunning(v bool) {
	m.mu.Lock()
	m.scrubStats.Running = v
	if v {
		m.scrubStats.StartedAt = time.Now()
	}
	m.mu.Unlock()
}

func (m *Multi) rotateScrubStats(d time.Duration) {
	m.mu.Lock()
	m.scrubStats.LastDuration = d
	m.scrubStats.LastFixedErrors = m.scrubStats.FixedErrors
	m.scrubStats.LastUnfixedErrors = m.scrubStats.UnfixedErrors
	m.scrubStats.LastOkay = m.scrubStats.Okay
	m.scrubStats.LastStartedAt = m.scrubStats.StartedAt
	m.scrubStats.FixedErrors = 0
	m.scrubStats.UnfixedErrors = 0
	m.scrubStats.Okay = 0
	m.scrubStats.StartedAt = time.Time{}
	m.mu.Unlock()
}

func (m *Multi) incrementScrubErrors(fixed bool) {
	m.mu.Lock()
	if fixed {
		m.scrubStats.FixedErrors++
	} else {
		m.scrubStats.UnfixedErrors++
	}
	m.mu.Unlock()
}

func (m *Multi) incrementOkay() {
	m.mu.Lock()
	m.scrubStats.Okay++
	m.mu.Unlock()
}

func (m *Multi) scrubFile(path string) {
	type failure struct {
		name string
		tgt  store.Target
		err  error
	}

	type tgtChunk struct {
		*chunk.Chunk
		name string
		tgt  store.Target
	}

	prefix := path + ".v1d"

	// read all chunks for this prefix, keeping track of chunks
	// that failed to read
	var chunks []tgtChunk
	var failed []failure
	for _, tgt := range m.targets {
		l, err := tgt.Search(prefix)
		if err != nil {
			log.Printf("[scrub] search on %v returned %v", path, tgt.Name(), err)
			m.incrementScrubErrors(false)
			return
		}

		for _, fi := range l {
			if fi.IsDir {
				continue
			}

			data, err := tgt.Get(fi.Name)
			if err != nil {
				failed = append(failed, failure{fi.Name, tgt, err})
				continue
			}

			chunk := new(chunk.Chunk)
			err = chunk.UnmarshalBinary(data)
			if err != nil {
				failed = append(failed, failure{fi.Name, tgt, err})
				continue
			}

			chunks = append(chunks, tgtChunk{chunk, fi.Name, tgt})
		}
	}

	// find the most prominent file content under this name
	counts := make(map[chunk.FileInfo]int)
	for _, chunk := range chunks {
		counts[chunk.FileInfo]++
	}

	var bestInfo *chunk.FileInfo
	for fi, count := range counts {
		if bestInfo == nil || count > counts[*bestInfo] {
			n := new(chunk.FileInfo)
			*n = fi
			bestInfo = n
		}
	}

	// split chunks into correct chunks (into "chunks" var) and other chunks
	var otherChunks []tgtChunk
	for i := 0; i < len(chunks); i++ {
		if chunks[i].FileInfo != *bestInfo {
			otherChunks = append(otherChunks, chunks[i])
			copy(chunks[i:], chunks[i+1:])
			chunks = chunks[:len(chunks)-1]
		}
	}

	byIndex := make(map[uint32][]tgtChunk)
	for _, chunk := range chunks {
		byIndex[chunk.ChunkIndex] = append(byIndex[chunk.ChunkIndex], chunk)
	}

	// remove any duplicate chunks (by chunk index)
	chunks = nil
	for _, theseChunks := range byIndex {
		for len(theseChunks) > 1 {
			chunk := theseChunks[0]

			log.Printf("[scrub] deleting duplicate chunk %v from %v", chunk.name, chunk.tgt.Name())
			err := chunk.tgt.Set(chunk.name, nil)
			if err != nil {
				log.Printf("[scrub] couldn't delete duplicate chunk %v from %v: %v", chunk.name, chunk.tgt.Name(), err)
			}
			m.incrementScrubErrors(err == nil)

			theseChunks = theseChunks[1:]
		}

		chunks = append(chunks, theseChunks...)
	}

	// ensure the one we picked is decodable
	if len(chunks) < int(bestInfo.DataChunks) {
		log.Printf("[scrub] not enough chunks available to recover %v", path)
		m.incrementScrubErrors(false)
		return
	}

	// remove any other chunks from other content versions
	for _, other := range otherChunks {
		err := other.tgt.Set(other.name, nil)
		if err != nil {
			log.Printf("[scrub] Couldn't remove extraneous chunk %v from %v: %v")
		}
		m.incrementScrubErrors(err == nil)
	}

	rebuild := false
	if len(chunks) < int(bestInfo.DataChunks+bestInfo.ParityChunks) {
		log.Printf("[scrub] %v is missing a chunk, rebuilding", path)
		rebuild = true
	}

	m.mu.Lock()
	cfg := m.config
	m.mu.Unlock()
	if int(bestInfo.DataChunks) != cfg.ChunksNeed || int(bestInfo.ParityChunks+bestInfo.DataChunks) != cfg.ChunksTotal {
		log.Printf("[scrub] %v has incorrect redundancy values (is %v+%v, want %v+%v)", path, bestInfo.DataChunks, bestInfo.ParityChunks, cfg.ChunksNeed, cfg.ChunksTotal-cfg.ChunksNeed)
		rebuild = true
	}

	if rebuild {
		data, err := m.Get(path)
		if err != nil {
			log.Printf("[scrub] couldn't get %v during rebuild: %v", path, err)
			m.incrementScrubErrors(false)
			return
		}

		err = m.Set(path, data)
		if err != nil {
			log.Printf("[scrub] couldn't set %v during rebuild: %v", path, err)
			m.incrementScrubErrors(false)
			return
		}

		m.incrementScrubErrors(true)
	} else {
		m.incrementOkay()
	}
}
