package multi

import (
	"git.encryptio.com/slime/lib/chunk"
	"git.encryptio.com/slime/lib/store"
	"log"
	"math/rand"
	"time"
)

type ScrubStats struct {
	Last struct {
		Duration time.Duration `json:"duration,omitempty"`
		Fixed    int64         `json:"fixed,omitempty"`
		Unfixed  int64         `json:"unfixed,omitempty"`
		Okay     int64         `json:"okay,omitempty"`
		Started  time.Time     `json:"started,omitempty"`
		Duty     float64       `json:"duty,omitempty"`
	} `json:"last"`

	Current struct {
		Running bool      `json:"running"`
		Started time.Time `json:"started,omitempty"`
		Fixed   int64     `json:"fixed,omitempty"`
		Unfixed int64     `json:"unfixed,omitempty"`
		Okay    int64     `json:"okay,omitempty"`
		DutyNum int64     `json:"duty_num,omitempty"`
		DutyDen int64     `json:"duty_den,omitempty"`
	} `json:"current,omitempty"`
}

func (m *Multi) GetScrubStats() ScrubStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.scrubStats
}

func (m *Multi) scrubLoop() {
	defer func() {
		m.done <- struct{}{}
	}()
	for !m.isStopping() {
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
		if fi.IsDir {
			m.scrubRec(fi.Name + "/")
		} else {
			m.scrubSleep(func() {
				m.scrubFile(fi.Name)
			})
		}

		if m.isStopping() {
			return
		}
	}
}

func (m *Multi) scrubSleep(inner func()) {
	m.mu.Lock()
	fpm := m.config.ScrubFilesPerMinute
	m.mu.Unlock()

	start := time.Now()
	until := start.Add(time.Minute / time.Duration(fpm))

	inner()

	end := time.Now()

	d := until.Sub(end)
	if d > 0 {
		time.Sleep(d)
	}

	after := time.Now()

	m.mu.Lock()
	m.scrubStats.Current.DutyNum += int64(end.Sub(start))
	m.scrubStats.Current.DutyDen += int64(after.Sub(start))
	m.mu.Unlock()
}

func (m *Multi) setScrubRunning(v bool) {
	m.mu.Lock()
	m.scrubStats.Current.Running = v
	if v {
		m.scrubStats.Current.Started = time.Now()
	}
	m.mu.Unlock()
}

func (m *Multi) rotateScrubStats(d time.Duration) {
	m.mu.Lock()

	l := &m.scrubStats.Last
	c := &m.scrubStats.Current

	l.Duration = d
	l.Fixed = c.Fixed
	l.Unfixed = c.Unfixed
	l.Okay = c.Okay
	l.Started = c.Started
	l.Duty = float64(c.DutyNum) / float64(c.DutyDen+1)

	c.Started = time.Time{}
	c.Fixed = 0
	c.Unfixed = 0
	c.Okay = 0
	c.DutyNum = 0
	c.DutyDen = 0

	m.mu.Unlock()
}

func (m *Multi) incrementScrubErrors(fixed bool) {
	m.mu.Lock()
	if fixed {
		m.scrubStats.Current.Fixed++
	} else {
		m.scrubStats.Current.Unfixed++
	}
	m.mu.Unlock()
}

func (m *Multi) incrementOkay() {
	m.mu.Lock()
	m.scrubStats.Current.Okay++
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
		res, err := m.Get(path)
		if err != nil {
			log.Printf("[scrub] couldn't get %v during rebuild: %v", path, err)
			m.incrementScrubErrors(false)
			return
		}

		err = m.Set(path, res.Data)
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
