// Package ledger 积分账本：记录积分来源（签到/任务/旅行/礼包）与去处（per-request 消耗），
// 分账号统计。内存为主 + 原子落盘持久化（与 pool/state 同模式），重启不丢账。
package ledger

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	// archive 裁剪归档：被上限挤出窗口的条目按【小时】折叠保留（每 uid 一条）。
	//
	// 为什么必须留：Summarize/Totals 只遍历 s.entries，若裁剪时直接丢弃，
	// 汇总就只剩窗口内的数——入账看着会「倒扣」，净额跟着缩水。这不是
	// 「近期流水」的定位问题，而是汇总口径错误：面板「积分明细」的
	// 入账/消耗/净额本该是累计值。
	//
	// 为什么按小时而不是单个标量：界面有 24h/7d/30d/全部 四档时间范围，
	// 只留标量的话「近 24 小时」会把全部历史算进去——修一个错引入另一个。
	// 小时粒度与 internal/usage 的分片口径一致（同一 hourLayout）。
	archive []archiveShard
	max     int // 上限（滚动丢弃最旧）
	dirty   bool
	path    string     // 持久化文件；空 = 纯内存
	saveMu  sync.Mutex // 落盘串行化
}

// archiveShard 归档分片：(小时, 账号) 维度的汇总。
//
// Scope 形如 "h:2026-09-22T13"（本地时区整点）。超过 archiveHourKeep 的小时片
// 在落盘前折叠成日片 "d:2026-09-22"，把长期体积压到「账号数 × 天数」量级
// （用量模块 Rollup 同思路；那边 90 天，这边对齐界面最大窗口 30 天）。
type archiveShard struct {
	Scope     string  `json:"s"`           // "h:..." / "d:..."
	UID       string  `json:"u"`           // 账号 uid
	Nick      string  `json:"n,omitempty"` // 昵称（折叠时取最后一个非空）
	Inflow    float64 `json:"i,omitempty"` // 入账合计（正数）
	Outflow   float64 `json:"o,omitempty"` // 消耗合计（正数，绝对值和）
	ChatCount int     `json:"c,omitempty"` // 对话请求数
}

// archiveHourKeep 小时内归档的保留时长。取 30 天：界面时间范围最大档是
// 720h（近 30 天），其内保持小时粒度；更早的只看「全部」，日粒度足够。
const archiveHourKeep = 30 * 24 * time.Hour

// shardTime 解析分片时间（去掉 "h:"/"d:" 前缀）。解析失败返回零值。
func shardTime(scope string) time.Time {
	if len(scope) < 3 {
		return time.Time{}
	}
	body := scope[2:]
	if strings.HasPrefix(scope, "h:") {
		if t, err := time.ParseInLocation("2006-01-02T15", body, time.Local); err == nil {
			return t
		}
		return time.Time{}
	}
	if t, err := time.ParseInLocation("2006-01-02", body, time.Local); err == nil {
		return t
	}
	return time.Time{}
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
	// Archive 裁剪归档（被上限挤出窗口的条目的小时/日折叠）。
	// 必须持久化：否则重启后汇总又只剩窗口内的数，等于裁剪 bug 复现一遍。
	Archive []archiveShard `json:"archive,omitempty"`
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
	s.archive = st.Archive
	// 启动时折叠一次：停机期间跨过 30 天的小时片趁此收敛为日片。
	s.rollupArchiveLocked(time.Now())
	log.Printf("ledger: 恢复 %d 条积分流水（归档 %d 个分片）", len(s.entries), len(s.archive))
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
		//
		// 被挤出的条目**先归档再丢弃**：归档保留其入账/消耗/请求数，
		// 汇总（Summarize/Totals）把归档一并计入，否则界面数字会随
		// 裁剪「倒扣」（见 archiveShard 注释）。
		cut := n - s.max
		s.archiveLocked(s.entries[:cut])
		s.entries = append([]Entry(nil), s.entries[cut:]...)
	}
	s.dirty = true
	s.mu.Unlock()

	go s.saveSoon()
}

// AppendOnce 幂等入账：e.Ref 非空且已入过账则丢弃并返回 false。
// 供「按上游权威记录回补」使用——该同步每次拉全量、可重复执行，
// 靠 Ref 去重；Ref 为空时退化为普通 Append（返回 true）。
// archiveLocked 把被裁剪的条目折叠进归档（调用方须持 s.mu）。
//
// 按 (小时, uid) 累加：同一小时内同账号的多条合成一条，归档体积只与
// 「活跃小时数 × 账号数」有关，不随请求量增长。
func (s *Store) archiveLocked(evicted []Entry) {
	if len(evicted) == 0 {
		return
	}
	idx := make(map[string]int, len(s.archive))
	for i := range s.archive {
		idx[s.archive[i].Scope+"\x1f"+s.archive[i].UID] = i
	}
	for _, e := range evicted {
		scope := "h:" + e.At.Format("2006-01-02T15")
		key := scope + "\x1f" + e.UID
		i, ok := idx[key]
		if !ok {
			s.archive = append(s.archive, archiveShard{Scope: scope, UID: e.UID})
			i = len(s.archive) - 1
			idx[key] = i
		}
		a := &s.archive[i]
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
	}
}

// rollupArchiveLocked 把超出 archiveHourKeep 的小时片折叠成日片（调用方须持 s.mu）。
// 与 internal/usage 的 Rollup 同思路：长期数据只保留日粒度，体积有界。
func (s *Store) rollupArchiveLocked(now time.Time) {
	cutoff := now.Add(-archiveHourKeep)
	type dayKey struct{ scope, uid string }
	out := make([]archiveShard, 0, len(s.archive))
	// 索引必须**预置已存在的日片**：折叠是反复发生的，若只跟踪本次新建的，
	// 上一轮留下的 d:X 与本次又折出的 d:X 会同时存在——同一 (日, 账号)
	// 两条分片，汇总即重复计数。预置后一律累加到既有分片上。
	idx := map[dayKey]int{}
	for _, a := range s.archive {
		if strings.HasPrefix(a.Scope, "d:") {
			idx[dayKey{a.Scope, a.UID}] = -1 // 先标记，待下面复制时回填真实下标
		}
	}
	// 先把所有日片原样搬进 out 并记录下标。
	for _, a := range s.archive {
		if strings.HasPrefix(a.Scope, "d:") {
			idx[dayKey{a.Scope, a.UID}] = len(out)
			out = append(out, a)
		}
	}
	// 再把小时片折叠进去：过期的并入所在日，未过期的原样保留。
	for _, a := range s.archive {
		t := shardTime(a.Scope)
		if !strings.HasPrefix(a.Scope, "h:") || t.IsZero() || !t.Before(cutoff) {
			if strings.HasPrefix(a.Scope, "h:") {
				out = append(out, a)
			}
			continue
		}
		dk := dayKey{"d:" + t.Format("2006-01-02"), a.UID}
		i, ok := idx[dk]
		if !ok || i < 0 {
			idx[dk] = len(out)
			out = append(out, archiveShard{
				Scope: dk.scope, UID: a.UID, Nick: a.Nick,
				Inflow: a.Inflow, Outflow: a.Outflow, ChatCount: a.ChatCount,
			})
			continue
		}
		out[i].Inflow += a.Inflow
		out[i].Outflow += a.Outflow
		out[i].ChatCount += a.ChatCount
		if a.Nick != "" {
			out[i].Nick = a.Nick
		}
	}
	s.archive = out
}

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
	// 折叠放在落盘路径（而非每次 Append）：saveSoon 由 saveMu 串行且有 500ms
	// 去抖，实际频率约 2 次/秒，折叠成本可忽略；放进 Append 则会随每条记录
	// 反复扫描归档。
	s.rollupArchiveLocked(time.Now())
	entries := make([]Entry, len(s.entries))
	copy(entries, s.entries)
	archive := make([]archiveShard, len(s.archive))
	copy(archive, s.archive)
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
	raw, err := json.MarshalIndent(snapshot{Entries: entries, Refs: refs, Archive: archive}, "", " ")
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
	Inflow    float64 `json:"inflow"`     // 来源合计（正数）
	Outflow   float64 `json:"outflow"`    // 去处合计（正数，取绝对值便于展示）
	Net       float64 `json:"net"`        // 净额 = inflow - outflow
	ChatCount int     `json:"chat_count"` // 对话请求数
}

// Summarize 分账号汇总（时间范围同 Query）。
func (s *Store) Summarize(q Query) []AccountSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	acc := map[string]*AccountSummary{}
	var order []string
	// 先并入裁剪归档：被上限挤出窗口的条目在这里仍有账，否则汇总会随
	// 裁剪「倒扣」（入账缩水、净额失真）——账本的定位是「近期流水」，
	// 但汇总口径必须是累计值。
	for i := range s.archive {
		a := &s.archive[i]
		if !q.Since.IsZero() {
			t := shardTime(a.Scope)
			// 分片起点早于窗口下界即跳过；日片按日起点比较（与 usage 同口径）。
			if !t.IsZero() && t.Before(q.Since) {
				continue
			}
		}
		if !q.Until.IsZero() {
			t := shardTime(a.Scope)
			if !t.IsZero() && t.After(q.Until) {
				continue
			}
		}
		if q.UID != "" && a.UID != q.UID {
			continue
		}
		cur := acc[a.UID]
		if cur == nil {
			cur = &AccountSummary{UID: a.UID, Nick: a.Nick}
			acc[a.UID] = cur
			order = append(order, a.UID)
		}
		cur.Inflow += a.Inflow
		cur.Outflow += a.Outflow
		cur.ChatCount += a.ChatCount
		if a.Nick != "" {
			cur.Nick = a.Nick
		}
	}
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
	}
	sort.Strings(order)
	out := make([]AccountSummary, 0, len(order))
	for _, uid := range order {
		a := acc[uid]
		// Net 由两项推出而非逐条累加：归档只有合计（无逐条 delta），
		// 逐条累加会漏掉归档部分，净额与「入账−消耗」对不上。
		a.Net = a.Inflow - a.Outflow
		out = append(out, *a)
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
