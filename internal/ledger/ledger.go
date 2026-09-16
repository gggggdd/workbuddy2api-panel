// Package ledger 积分账本：记录积分来源（签到/任务/旅行/礼包）与去处（per-request 消耗），
// 分账号统计。内存为主 + 原子落盘持久化（与 pool/state 同模式），重启不丢账。
package ledger

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Kind 条目类型。方向由 Delta 符号决定：正 = 来源（入账），负 = 去处（消耗）。
type Kind string

const (
	KindChat         Kind = "chat"         // 去处：对话消耗（usage.credit）
	KindCheckin      Kind = "checkin"      // 来源：每日签到
	KindTask         Kind = "task"         // 来源：任务奖励（含一键完成自动领奖）
	KindTravel       Kind = "travel"       // 来源：猫猫旅行到站奖励
	KindLottery      Kind = "lottery"      // 来源：抽奖积分（开学季转盘 / 连登抽奖）
	KindGift         Kind = "gift"         // 来源：新手礼包
	KindCompensation Kind = "compensation" // 来源：活动补偿
	KindAdjust       Kind = "adjust"       // 校准：账本推算与余额快照的未知差额
)

// Entry 单条积分流水。
type Entry struct {
	At      time.Time `json:"at"`                // 发生时刻
	UID     string    `json:"uid"`               // 账号 uid（完整）
	Nick    string    `json:"nick,omitempty"`    // 账号昵称（记入时快照）
	Kind    Kind      `json:"kind"`              // 类型
	Delta   float64   `json:"delta"`             // 变动量：正=入账，负=消耗
	Model   string    `json:"model,omitempty"`   // chat: 模型名
	Task    string    `json:"task,omitempty"`    // task: 任务 code
	Member  string    `json:"member,omitempty"`  // chat: 成员密钥 ID（管理员/直连为空）
	Balance float64   `json:"balance,omitempty"` // 记账后该账号已知余额（可得时填）
	Note    string    `json:"note,omitempty"`    // 备注（如 adjust 的差额说明）
	// Ref 幂等键：非空时，同 Ref 的条目只入账一次。用于「按上游权威记录回补」
	// 这种可重复执行的同步（每次拉全量，靠 Ref 去重），空 = 不去重。
	Ref string `json:"ref,omitempty"`
}

// Store 账本。并发安全；写入内存 + 去抖落盘（合并写，避免高频 chat 拖慢请求路径）。
type Store struct {
	mu      sync.Mutex
	entries []Entry
	refs    map[string]bool // 已入账的 Entry.Ref（幂等同步去重用）；空 Ref 不入此表
	max     int             // 上限（滚动丢弃最旧）
	dirty   bool
	path    string     // 持久化文件；空 = 纯内存
	saveMu  sync.Mutex // 落盘串行化
}

// DefaultMax 流水保留上限：超出后丢弃最旧记录（溢出部分不保留）。
//
// 取值取向：账本定位是「近期流水」，不是长期存档——chat 消耗约 1000 条/天，
// 保留 1000 条即约一天窗口，足够排查「这次请求扣了多少」，且让落盘文件与
// 面板列表都保持在可读长度。长期统计请用 cmd/credit（按上游 TotalDosage 对账）。
const DefaultMax = 1000

// snapshot 账本落盘格式。
//
// Refs 单独持久化：幂等键不能只从存活条目重建——条目被上限裁掉后若 Ref 一起
// 消失，可重复执行的上游同步（school rewards）会在下一轮把同一条记录又补回来，
// 截断就形同虚设，且旧奖励会以「最新条目」的数组位置重新出现在列表顶部。
// 目前仅开学季同步写 Ref（活动期内约 150 条），规模可控。
type snapshot struct {
	Entries []Entry  `json:"entries"`
	Refs    []string `json:"refs,omitempty"`
}

// New 创建账本。maxN <=0 时取 DefaultMax；path 为空则不持久化。
func New(path string, maxN int) *Store {
	if maxN <= 0 {
		maxN = DefaultMax
	}
	s := &Store{max: maxN, path: path, refs: map[string]bool{}}
	if path != "" {
		s.load()
	}
	return s
}

// load 启动时恢复历史账目。
func (s *Store) load() {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return // 首次运行无文件，正常
	}
	var st snapshot
	if err := json.Unmarshal(raw, &st); err != nil {
		log.Printf("ledger: 恢复失败（忽略，账本重开）: %v", err)
		return
	}
	// 幂等键先从「落盘 Refs + 全部条目」重建，再截断——两处顺序不能换：
	// 被上限裁掉的条目也必须留住去重键，否则下一轮上游同步会把它们当成
	// 没记过的新记录补回来（还会以数组末尾位置冒到列表顶部），截断即失效。
	for _, ref := range st.Refs {
		s.refs[ref] = true
	}
	for _, e := range st.Entries {
		if e.Ref != "" {
			s.refs[e.Ref] = true
		}
	}
	entries := st.Entries
	// 上限收缩（如旧文件留下更多条）时按同一规则截断：只保留最新的 max 条。
	// 不截断的话，调小上限后旧数据会一直滞留，直到被新记录逐条挤出去。
	if n := len(entries); n > s.max {
		entries = append([]Entry(nil), entries[n-s.max:]...)
		log.Printf("ledger: 恢复 %d 条，按上限 %d 截断", n, s.max)
	}
	s.entries = entries
	log.Printf("ledger: 恢复 %d 条积分流水", len(s.entries))
}

// Append 记一条流水。delta 必须非零（零变动不入账）。
func (s *Store) Append(e Entry) {
	if e.Delta == 0 {
		return
	}
	if e.At.IsZero() {
		e.At = time.Now()
	}
	s.mu.Lock()
	s.entries = append(s.entries, e)
	if n := len(s.entries); n > s.max {
		// 溢出即裁掉最旧的，保留最新 max 条（与 load 的截断规则一致）。
		//
		// 注意不要写回「丢弃 1/8」的滚动步长：那种写法在 n 刚过 max 时
		// 起点是 n-cut，保留条数只有 cut（max/8），会把账本砸到 1/8。
		// 这里保留条数有明确上界，语义与「只保留 max 条」一致，便于推理。
		s.entries = append([]Entry(nil), s.entries[n-s.max:]...)
	}
	s.dirty = true
	s.mu.Unlock()

	go s.saveSoon()
}

// AppendOnce 幂等入账：e.Ref 非空且已入过账则丢弃并返回 false。
// 供「按上游权威记录回补」使用——该同步每次拉全量、可重复执行，
// 靠 Ref 去重；Ref 为空时退化为普通 Append（返回 true）。
func (s *Store) AppendOnce(e Entry) bool {
	if e.Delta == 0 {
		return false
	}
	if e.Ref != "" {
		s.mu.Lock()
		dup := s.refs[e.Ref]
		if !dup {
			s.refs[e.Ref] = true
		}
		s.mu.Unlock()
		if dup {
			return false
		}
	}
	s.Append(e)
	return true
}

// saveSoon 落盘去抖：500ms 内的连续写入合并为一次。
func (s *Store) saveSoon() {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	time.Sleep(500 * time.Millisecond)
	s.mu.Lock()
	if !s.dirty {
		s.mu.Unlock()
		return
	}
	entries := make([]Entry, len(s.entries))
	copy(entries, s.entries)
	// 幂等键随盘保存（含已被上限裁掉的条目）：否则重启后同步会把裁掉的奖励补回来。
	// 规模有界——仅开学季权威同步写 Ref（活动期内每号每日数条），长期不增长。
	refs := make([]string, 0, len(s.refs))
	for r := range s.refs {
		refs = append(refs, r)
	}
	s.dirty = false
	s.mu.Unlock()
	sort.Strings(refs) // 稳定输出，避免同一集合因 map 顺序不同而反复重写

	if s.path == "" {
		return
	}
	raw, err := json.MarshalIndent(snapshot{Entries: entries, Refs: refs}, "", " ")
	if err != nil {
		return
	}
	tmp := s.path + ".tmp"
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		log.Printf("ledger: 落盘失败: %v", err)
		return
	}
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		log.Printf("ledger: 落盘失败: %v", err)
		return
	}
	if err := os.Rename(tmp, s.path); err != nil {
		log.Printf("ledger: 落盘失败: %v", err)
	}
}

// Query 查询条件。零值字段不过滤；Since/Until 为空不限时间。
type Query struct {
	UID    string // 按账号
	Kind   Kind   // 按类型
	Member string // 按成员（仅 chat 类有意义）
	Since  time.Time
	Until  time.Time
	Limit  int // 返回条数上限（时间倒序取最近 N 条；<=0 不限）
}

// List 按条件查询（时间倒序）。
func (s *Store) List(q Query) []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Entry
	for i := len(s.entries) - 1; i >= 0; i-- {
		e := s.entries[i]
		if q.UID != "" && e.UID != q.UID {
			continue
		}
		if q.Kind != "" && e.Kind != q.Kind {
			continue
		}
		if q.Member != "" && e.Member != q.Member {
			continue
		}
		if !q.Since.IsZero() && e.At.Before(q.Since) {
			continue
		}
		if !q.Until.IsZero() && e.At.After(q.Until) {
			continue
		}
		out = append(out, e)
		if q.Limit > 0 && len(out) >= q.Limit {
			break
		}
	}
	return out
}

// AccountSummary 分账号汇总：入账/消耗/净额。
type AccountSummary struct {
	UID       string  `json:"uid"`
	Nick      string  `json:"nick"`
	Inflow    float64 `json:"inflow"`      // 来源合计（正数）
	Outflow   float64 `json:"outflow"`     // 去处合计（正数，取绝对值便于展示）
	Net       float64 `json:"net"`         // 净额 = inflow - outflow
	ChatCount int     `json:"chat_count"`  // 对话请求数
}

// Summarize 分账号汇总（时间范围同 Query）。
func (s *Store) Summarize(q Query) []AccountSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc := map[string]*AccountSummary{}
	var order []string
	for _, e := range s.entries {
		if !q.Since.IsZero() && e.At.Before(q.Since) {
			continue
		}
		if !q.Until.IsZero() && e.At.After(q.Until) {
			continue
		}
		a := acc[e.UID]
		if a == nil {
			a = &AccountSummary{UID: e.UID, Nick: e.Nick}
			acc[e.UID] = a
			order = append(order, e.UID)
		}
		if e.Nick != "" {
			a.Nick = e.Nick
		}
		if e.Delta > 0 {
			a.Inflow += e.Delta
		} else {
			a.Outflow += -e.Delta
			if e.Kind == KindChat {
				a.ChatCount++
			}
		}
		a.Net += e.Delta
	}
	sort.Strings(order)
	out := make([]AccountSummary, 0, len(order))
	for _, uid := range order {
		out = append(out, *acc[uid])
	}
	return out
}

// Totals 全局汇总（时间范围内）。
func (s *Store) Totals(q Query) (inflow, outflow float64) {
	for _, a := range s.Summarize(q) {
		inflow += a.Inflow
		outflow += a.Outflow
	}
	return
}
