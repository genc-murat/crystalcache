package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	cmdCount      int64
	startTime     time.Time
	activeConns   int32
	totalCommands int64
	commandStats  map[string]*CommandStats
	mu            sync.RWMutex
}

type CommandStats struct {
	Calls        int64
	TotalTime    int64
	LastExecTime time.Time
}

func NewMetrics() *Metrics {
	return &Metrics{
		startTime:    time.Now(),
		commandStats: make(map[string]*CommandStats),
	}
}

func (m *Metrics) IncrCommandCount() {
	atomic.AddInt64(&m.cmdCount, 1)
}

func (m *Metrics) GetCommandCount() int64 {
	return atomic.LoadInt64(&m.cmdCount)
}

func (m *Metrics) AddCommandExecution(cmd string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	stats, exists := m.commandStats[cmd]
	if !exists {
		stats = &CommandStats{}
		m.commandStats[cmd] = stats
	}

	stats.Calls++
	stats.TotalTime += duration.Nanoseconds()
	stats.LastExecTime = time.Now()
}

func (m *Metrics) GetStats() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["uptime_in_seconds"] = int(time.Since(m.startTime).Seconds())
	stats["total_commands_processed"] = m.GetCommandCount()
	stats["connected_clients"] = atomic.LoadInt32(&m.activeConns)

	cmdStats := make(map[string]map[string]interface{})
	for cmd, stat := range m.commandStats {
		cmdStats[cmd] = map[string]interface{}{
			"calls":          stat.Calls,
			"total_time_us":  stat.TotalTime / 1000,
			"avg_time_us":    stat.TotalTime / stat.Calls / 1000,
			"last_exec_time": stat.LastExecTime,
		}
	}
	stats["commandstats"] = cmdStats

	return stats
}
