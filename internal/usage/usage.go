// Package usage 定期采样各账号的上游计费用量（used 计数），提供固定时间窗口内
// 的「积分总使用量」观测，供面板看板展示。
//
// 为什么用上游 used 而不是累加每次请求的 usage.credit：
//   - used 是账单口径（used + remain = size），涵盖全部消耗来源——包括网关
//     定时任务（夜猫子等）与任何非 /v1/chat/completions 路径的花费；
//   - 逐请求累加只能覆盖经过网关的请求，且进程重启即丢失；
//   - used 单调递增，因此跨重启仍然有效（历史样本不因重启失效）。
//
// 已知粒度限制：上游计费有分钟级延迟，used 为整数分，故窗口值存在 ±数分的
// 采样误差；不适用于逐请求的精确计费。
package usage

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"workbuddy2api/internal/pool"
	"workbuddy2api/internal/upstream"
)

const (
	// SampleInterval 采样周期。上游计费计数分钟级延迟，5 分钟足够，
	// 且请求量可忽略（每账号每 5 分钟一次余额查询）。
	SampleInterval = 5 * time.Minute
	// retain 样本保留时长：覆盖 24h 窗口并留冗余。
	retain = 48 * time.Hour
	// maxSamples 硬上限，防止异常配置下无限增长。
	maxSamples = 2000
)

// sample 单次采样：uid → 该账号的 used 累计值。
type sample struct {
	T        int64            `json:"t"`        // Unix 秒
	Accounts map[string]int64 `json:"accounts"` // uid → used
}

// Window 一个时间窗口内的使用量统计。
type Window struct {
	Used     int64   `json:"used"`      // 窗口内消耗合计
	Seconds  int64   `json:"seconds"`   // 实际覆盖时长
	Complete bool    `json:"complete"`  // true = 有覆盖整个窗口的样本；false = 历史不足
	PerHour  float64 `json:"per_hour"`  // 平均每小时消耗
	Since    string  `json:"since"`     // 基准样本时刻（RFC3339）
	Accounts int     `json:"accounts"`  // 参与统计的账号数
}

// Stats 看板数据。
type Stats struct {
	UpdatedAt time.Time `json:"updated_at"` // 最近一次采样时刻
	Samples   int       `json:"samples"`    // 当前保留的样本数
	Oldest    string    `json:"oldest"`     // 最老样本时刻（RFC3339）
	TotalUsed int64     `json:"total_used"` // 各账号 used 合计（账单累计值）
	Window5h  *Window   `json:"window_5h"`
	Window24h *Window   `json:"window_24h"`
}

// Tracker 采样器：周期查询各账号 used 并留存样本序列。
type Tracker struct {
	pool     *pool.Pool
	upstream *upstream.Client
	path     string

	mu      sync.Mutex
	samples []sample
	lastErr string
}

// New 构建采样器。path 为空表示不持久化（仅内存）。
func New(p *pool.Pool, up *upstream.Client, path string) *Tracker {
	t := &Tracker{pool: p, upstream: up, path: path}
	t.load()
	return t
}

// load 从磁盘恢复样本（损坏或不存在时静默从头开始）。
func (t *Tracker) load() {
	if t.path == "" {
		return
	}
	raw, err := os.ReadFile(t.path)
	if err != nil {
		return
	}
	var samples []sample
	if json.Unmarshal(raw, &samples) != nil {
		return
	}
	t.samples = trim(samples, time.Now())
}

// save 原子落盘（tmp + rename）；失败只记日志，不影响采样主流程。
func (t *Tracker) save() {
	if t.path == "" {
		return
	}
	raw, err := json.Marshal(t.samples)
	if err != nil {
		return
	}
	tmp := t.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		log.Printf("usage: 落盘失败 %v", err)
		return
	}
	if err := os.Rename(tmp, t.path); err != nil {
		log.Printf("usage: 替换失败 %v", err)
	}
}

// trim 丢弃超出保留期的样本与超出硬上限的最老样本。
func trim(samples []sample, now time.Time) []sample {
	cut := now.Add(-retain).Unix()
	out := samples[:0]
	for _, s := range samples {
		if s.T >= cut {
			out = append(out, s)
		}
	}
	if len(out) > maxSamples {
		out = out[len(out)-maxSamples:]
	}
	return out
}

// Start 启动周期采样循环（阻塞直到 ctx 取消）。启动时立即采一次，
// 让看板在进程起来后很快就有基线。
func (t *Tracker) Start(ctx context.Context) {
	t.SampleNow()
	ticker := time.NewTicker(SampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.SampleNow()
		}
	}
}

// SampleNow 执行一轮采样：查询所有非禁用账号的 used，合并进样本序列。
//
// 查询失败的账号沿用上一次的已知值（而不是从样本中缺失）——缺失会让窗口内的
// 消耗被静默漏算；沿用则把误差限制在「该账号这段时间的增量」上。
// 全部账号都失败时不追加样本（避免写出一条全零的假样本）。
func (t *Tracker) SampleNow() {
	t.mu.Lock()
	prev := map[string]int64{}
	if n := len(t.samples); n > 0 {
		for k, v := range t.samples[n-1].Accounts {
			prev[k] = v
		}
	}
	t.mu.Unlock()

	cur := map[string]int64{}
	fresh := 0
	var firstErr error
	for _, st := range t.pool.List() {
		if st.Disabled {
			continue
		}
		a := t.pool.AuthByUID(st.UID)
		if a == nil {
			continue
		}
		u, err := t.upstream.UserResourceDetail(a)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if v, ok := prev[st.UID]; ok {
				cur[st.UID] = v // 沿用上次已知值
			}
			continue
		}
		cur[st.UID] = u.Used
		fresh++
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if fresh == 0 {
		if firstErr != nil {
			t.lastErr = firstErr.Error()
		}
		return // 无任何成功查询：不追加样本
	}
	t.lastErr = ""
	t.samples = append(t.samples, sample{T: time.Now().Unix(), Accounts: cur})
	t.samples = trim(t.samples, time.Now())
	t.save()
}

// Stats 返回当前看板数据（基于已保留的样本，不触发上游查询）。
func (t *Tracker) Stats() Stats {
	t.mu.Lock()
	samples := make([]sample, len(t.samples))
	copy(samples, t.samples)
	errMsg := t.lastErr
	t.mu.Unlock()

	out := Stats{Samples: len(samples)}
	if len(samples) == 0 {
		if errMsg != "" {
			out.Oldest = "采样失败：" + errMsg
		}
		return out
	}
	latest := samples[len(samples)-1]
	out.UpdatedAt = time.Unix(latest.T, 0)
	out.Oldest = time.Unix(samples[0].T, 0).Format(time.RFC3339)
	for _, v := range latest.Accounts {
		out.TotalUsed += v
	}
	out.Window5h = window(samples, 5*time.Hour)
	out.Window24h = window(samples, 24*time.Hour)
	return out
}

// window 计算最近 d 时长内的消耗：对每个账号，用「当前值 − 该账号在窗口起点的值」，
// 逐账号求和。逐账号比对而非直接比总量，是为了让窗口内新增/移除账号不会
// 制造虚假消耗（新账号没有基线 → 跳过；移除的账号不再出现在当前样本 → 计 0）。
//
// 负数增量（套餐周期重置导致 used 归零）钳为 0：宁可少算，不虚报。
func window(samples []sample, d time.Duration) *Window {
	latest := samples[len(samples)-1]
	cutoff := latest.T - int64(d.Seconds())

	w := &Window{}
	var used int64
	accounts := 0
	for uid, cur := range latest.Accounts {
		// 找该账号在 cutoff 之前最近的一个样本作基线；没有则用它最早出现的样本。
		var base int64
		var found bool
		for _, s := range samples {
			v, ok := s.Accounts[uid]
			if !ok {
				continue
			}
			if !found {
				base, found = v, true // 兜底：该账号最早可见值
			}
			if s.T <= cutoff {
				base = v // 覆盖为窗口起点之前的最新值
			}
		}
		if !found {
			continue
		}
		if delta := cur - base; delta > 0 {
			used += delta
		}
		accounts++
	}

	w.Used = used
	w.Accounts = accounts

	// 覆盖时长与完整性：以最老样本为界。
	span := latest.T - samples[0].T
	if span > int64(d.Seconds()) {
		span = int64(d.Seconds())
	}
	if span < 0 {
		span = 0
	}
	w.Seconds = span
	w.Complete = samples[0].T <= cutoff
	if !w.Complete {
		w.Since = time.Unix(samples[0].T, 0).Format(time.RFC3339)
	} else {
		w.Since = time.Unix(cutoff, 0).Format(time.RFC3339)
	}
	if w.Seconds > 0 {
		w.PerHour = float64(used) / (float64(w.Seconds) / 3600)
	}
	return w
}

// DefaultPath 由状态文件路径推导采样文件路径（同目录 usage.json）。
// state_file 为空时回落 ./data/usage.json。
func DefaultPath(stateFile string) string {
	if stateFile == "" {
		return filepath.Join("data", "usage.json")
	}
	return filepath.Join(filepath.Dir(stateFile), "usage.json")
}
