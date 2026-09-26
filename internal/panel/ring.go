// ring.go 固定容量的结构化日志环形缓冲（并发安全，实现 io.Writer）。
// main 把 log 包输出与 chat 表格日志经 MultiWriter 镜像进来，面板
// /panel/api/logs 读取快照；超容量时按频道优先级淘汰（见 trimLocked）。
//
// 每行入环时按前缀规则归类频道（chat=对话请求表格行 / task=任务动作 /
// sys=系统与其它），面板日志视图按频道筛选——对话流量大时任务结果不被冲掉。
package panel

import (
	"regexp"
	"strings"
	"sync"
	"time"
)

// 日志频道。
const (
	ChChat = "chat"
	ChTask = "task"
	ChSys  = "sys"
)

// LogEntry 单条日志（时间戳取写入时刻；log 包行的行首日期时间已被剥离）。
type LogEntry struct {
	TS   time.Time `json:"ts"`
	Ch   string    `json:"ch"`
	Text string    `json:"text"`
}

// taskPrefixes 任务动作日志的行首标识（scheduler 与 panel 的既有口径）。
var taskPrefixes = []string{
	"school ", "streak-bonus ", "travel ", "blackcat ", "lottery ",
	"checkin ", "activity ", "keepalive ", "balance ", "user-resource ",
	// blackcat 的窗口跳过行写作 "blackcat: …"（冒号），与上面 "blackcat " 的
	// 空格式是同一模块的两种写法——漏一个就会把夜猫子跳过判成 sys。
	"blackcat:",
	"panel: 任务", "panel: 一键", "panel: checkin", "panel: 手动",
	"panel: 队列", "panel: 券码",
	// 任务动作类面板日志：接受 / 领取 / 批量接受都是任务链路的结果行。
	// 原先只覆盖 "panel: 任务" 与 "panel: 队列"，这些动作行会掉进 sys 频道。
	"panel: 接受任务", "panel: 领取任务奖励", "panel: 全部接受",
	"panel: 批量接受失败", "panel: mp 批量接受失败", "panel: accept ",
	"panel: 定时",
}

// tsPrefixRe log 包默认 flags（日期 时间）产生的行首时间戳。
var tsPrefixRe = regexp.MustCompile(`^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} `)

// classifyLine 按行首特征归类频道。
func classifyLine(line string) string {
	if strings.HasPrefix(line, "| #") { // chat 表格日志（server/logging.go logChatRow）
		return ChChat
	}
	for _, p := range taskPrefixes {
		if strings.HasPrefix(line, p) {
			return ChTask
		}
	}
	return ChSys
}

// Ring 日志环形缓冲。
type Ring struct {
	mu      sync.Mutex
	entries []LogEntry
	cap     int
}

// NewRing 构建容量为 capacity 的日志环（非正值回退 500）。
func NewRing(capacity int) *Ring {
	if capacity <= 0 {
		capacity = 500
	}
	return &Ring{cap: capacity}
}

// Write 按 \n 切分入环（实现 io.Writer）。空行丢弃；超容量时按频道优先级淘汰。
func (r *Ring) Write(p []byte) (int, error) {
	now := time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, line := range strings.Split(strings.TrimRight(string(p), "\r\n"), "\n") {
		if line == "" {
			continue
		}
		text := tsPrefixRe.ReplaceAllString(line, "")
		r.entries = append(r.entries, LogEntry{TS: now, Ch: classifyLine(text), Text: text})
		if len(r.entries) > r.cap {
			r.trimLocked()
		}
	}
	return len(p), nil
}

// trimLocked 把缓冲压回容量上限，按频道分优先级淘汰（调用方须持锁）。
//
// 两层顺序，对应文件头的承诺「对话流量大时任务结果不被冲掉」：
//  1. task 上限 cap/2 —— 任务日志不得独占缓冲，否则对话视图会变成空白；
//  2. 仍溢出时优先淘汰非 task 行 —— chat 是每请求一行的洪水，而 task 记录的是
//     签到 / 猫猫旅行 / 抽奖这类「当天只看一次」的结果，冲掉就再也找不回来。
//
// 旧实现是单一队列严格 FIFO：chat 产量远高于任务动作，任务行几百条内即被挤掉。
func (r *Ring) trimLocked() {
	for r.countLocked(ChTask) > r.cap/2 {
		i := r.oldestLocked(ChTask)
		if i < 0 {
			break
		}
		r.removeLocked(i)
	}
	for len(r.entries) > r.cap {
		i := r.oldestNonTaskLocked()
		if i < 0 {
			i = 0
		}
		r.removeLocked(i)
	}
}

// oldestLocked 返回频道 ch 中最早一条的下标，没有则 -1。
func (r *Ring) oldestLocked(ch string) int {
	for i, e := range r.entries {
		if e.Ch == ch {
			return i
		}
	}
	return -1
}

// oldestNonTaskLocked 返回最早一条非 task 行的下标，全是 task 则 -1。
func (r *Ring) oldestNonTaskLocked() int {
	for i, e := range r.entries {
		if e.Ch != ChTask {
			return i
		}
	}
	return -1
}

// countLocked 统计频道 ch 的条数。
func (r *Ring) countLocked(ch string) int {
	n := 0
	for _, e := range r.entries {
		if e.Ch == ch {
			n++
		}
	}
	return n
}

// removeLocked 删除下标 i 的条目，保持其余顺序。
func (r *Ring) removeLocked(i int) {
	r.entries = append(r.entries[:i], r.entries[i+1:]...)
}

// Snapshot 按写入顺序返回缓冲内全部条目（拷贝，调用方可安全持有）。
func (r *Ring) Snapshot() []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]LogEntry, len(r.entries))
	copy(out, r.entries)
	return out
}
