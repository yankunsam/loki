package fetcher

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/grafana/loki/pkg/storage/chunk"
)

type chunkStats struct {
	stopMu  sync.Mutex
	stopped bool

	chunkMu   sync.Mutex
	chunkKeys map[string]int64

	timeMu     sync.Mutex
	timestamps map[int64]int64

	stopChan chan struct{}
}

func newChunkStats() *chunkStats {
	cs := &chunkStats{
		chunkKeys:  make(map[string]int64),
		timestamps: make(map[int64]int64),
		stopChan:   make(chan struct{}),
	}

	go cs.reportLoop()

	return cs
}

func (s *chunkStats) Record(chunks []chunk.Chunk, keys []string) {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	if s.stopped {
		return
	}

	go s.recordKeys(keys)
	go s.recordTS(chunks)
}

func (s *chunkStats) recordKeys(keys []string) {
	s.chunkMu.Lock()
	defer s.chunkMu.Unlock()

	for _, k := range keys {
		s.chunkKeys[k]++
	}
}

func (s *chunkStats) recordTS(chunks []chunk.Chunk) {
	s.timeMu.Lock()
	defer s.timeMu.Unlock()

	for _, k := range chunks {
		s.timestamps[k.Through.Unix()]++
	}
}

func (s *chunkStats) Stop() {
	s.stopMu.Lock()
	defer s.stopMu.Unlock()
	s.stopped = true
	close(s.stopChan)
}

func (s *chunkStats) writeFiles() {
	s.chunkMu.Lock()
	s.timeMu.Lock()

	defer s.chunkMu.Unlock()
	defer s.timeMu.Unlock()
	defer func() {
		s.timestamps = make(map[int64]int64)
		s.chunkKeys = make(map[string]int64)
	}()

	chunkFile, err := os.CreateTemp(os.TempDir(), "chunk-usage")
	if err != nil {
		fmt.Printf("EXPERIMENT FAILED: %s\n", err)
		return
	}

	for k, v := range s.chunkKeys {
		fmt.Fprintf(chunkFile, "%s,%d\n", k, v)
	}
	chunkFile.Close()

	tsFile, err := os.CreateTemp(os.TempDir(), "timestamp-usage")
	if err != nil {
		fmt.Printf("EXPERIMENT FAILED: %s\n", err)
		return
	}

	for k, v := range s.timestamps {
		fmt.Fprintf(tsFile, "%d,%d\n", k, v)
	}
	tsFile.Close()
}

func (s *chunkStats) reportLoop() {
	t := time.NewTimer(1 * time.Hour)
	select {
	case <-t.C:
		s.writeFiles()
	case <-s.stopChan:
		return
	}
}
