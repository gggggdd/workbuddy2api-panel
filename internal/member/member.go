// Package member 按成员（使用者）维度的密钥与用量记账。
//
// 背景：网关原本只有单一 api_key，无法区分"谁在用"。本包为每个成员签发
// 独立 Bearer 密钥，并在每次 /v1/chat/completions 后把该请求的
// usage.credit 记账到对应成员，从而支持"按成员监控使用量"。
//
// 为什么用 usage.credit 而不是上游账单 used：
//   - used 只能按账号（上游凭证）聚合，无法区分同一账号下的不同调用者；
//   - usage.credit 随每次响应返回，是唯一能归因到"请求发起者"的量。
//
// 精度限制（必须知情）：usage.credit 只有两位小数，极小请求会记 0；
// 上游账单 used 为整数分。因此成员维度是"相对消耗"的可靠观测，
// 不适合作为对外结算的精确账单。
package member

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"workbuddy2api/internal/httpauth"
)

// bucketSec 用量分桶粒度：15 分钟。5h 窗口取最近 20 桶、24h 取最近 96 桶，
// 因此窗口是"按 15 分钟对齐"的近似（实际跨度在 [hours-0.25h, hours] 之间）。
const (
	bucketSec = 900
	// bucketKeep 保留桶数：96（24h）+ 1（当前桶）。
	bucketKeep = 97
)

// bucket 单个时间桶的消耗累计。
type bucket struct {
	H int64   `json:"h"` // Unix 秒 / bucketSec
	C float64 `json:"c"` // 该桶内消耗合计
}

// Member 一个成员。
type Member struct {
	ID   string `json:"id"`   // 稳定标识（随机，不可变）
	Name string `json:"name"` // 显示名
	Note string `json:"note,omitempty"`
	Key  string `json:"key"` // Bearer 密钥（明文，与 config.api_key 同存法：管理员需分发）

	Created  int64 `json:"created"`
	LastUsed int64 `json:"last_used,omitempty"`

	TotalCredit float64 `json:"total_credit"` // 累计消耗
	Requests    int64   `json:"requests"`     // 累计请求数
	Errors      int64   `json:"errors"`       // 累计失败数

	Buckets []bucket `json:"buckets,omitempty"`
}

// Store 成员集合，带用量记账与原子持久化。
type Store struct {
	path string

	mu      sync.Mutex
	members []*Member
	dirty   bool
	lastErr string
}

// NewStore 加载成员文件（不存在则空集合）。path 为空表示仅内存。
func NewStore(path string) *Store {
	s := &Store{path: path}
	s.load()
	return s
}

func (s *Store) load() {
	if s.path == "" {
		return
	}
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return // 不存在：空集合
	}
	var list []*Member
	if json.Unmarshal(raw, &list) != nil {
		return // 损坏：从空开始，不阻塞启动
	}
	s.members = list
}

// Flush 原子落盘（tmp + rename），仅在脏时写。失败记录错误供面板显示。
func (s *Store) Flush() {
	s.mu.Lock()
	if !s.dirty || s.path == "" {
		s.mu.Unlock()
		return
	}
	raw, err := json.MarshalIndent(s.members, "", "  ")
	if err != nil {
		s.mu.Unlock()
		return
	}
	s.dirty = false
	s.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		s.setErr(err)
		return
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		s.setErr(err)
		return
	}
	if err := os.Rename(tmp, s.path); err != nil {
		s.setErr(err)
		return
	}
	s.setErr(nil)
}

func (s *Store) setErr(err error) {
	s.mu.Lock()
	if err == nil {
		s.lastErr = ""
	} else {
		s.lastErr = err.Error()
	}
	s.mu.Unlock()
}

// Count 成员数。
func (s *Store) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.members)
}

// genKey 生成成员密钥：wb- 前缀 + 24 字节 base64url（便于识别与手工复制）。
func genKey() string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand 失败极罕见；退回时间派生（仍带随机成分），不静默给出空密钥。
		return "wb-" + base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%d", time.Now().UnixNano())))
	}
	return "wb-" + base64.RawURLEncoding.EncodeToString(b)
}

func genID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("m%d", time.Now().UnixNano())
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

// Add 新建成员（name 去空白；空名回落 "成员"）。
func (s *Store) Add(name, note string) *Member {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "成员"
	}
	m := &Member{
		ID:      genID(),
		Name:    name,
		Note:    strings.TrimSpace(note),
		Key:     genKey(),
		Created: time.Now().Unix(),
	}
	s.mu.Lock()
	s.members = append(s.members, m)
	s.dirty = true
	s.mu.Unlock()
	s.Flush()
	return m
}

// Update 改成员名称/备注。
func (s *Store) Update(id, name, note string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.members {
		if m.ID == id {
			if n := strings.TrimSpace(name); n != "" {
				m.Name = n
			}
			m.Note = strings.TrimSpace(note)
			s.dirty = true
			return true
		}
	}
	return false
}

// Rotate 重新签发密钥（旧密钥立即失效）。
func (s *Store) Rotate(id string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.members {
		if m.ID == id {
			m.Key = genKey()
			s.dirty = true
			return m.Key, true
		}
	}
	return "", false
}

// ResetUsage 清零用量计数（保留成员与密钥）。
func (s *Store) ResetUsage(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.members {
		if m.ID == id {
			m.TotalCredit = 0
			m.Requests = 0
			m.Errors = 0
			m.Buckets = nil
			s.dirty = true
			return true
		}
	}
	return false
}

// Remove 删除成员。
func (s *Store) Remove(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, m := range s.members {
		if m.ID == id {
			s.members = append(s.members[:i], s.members[i+1:]...)
			s.dirty = true
			return true
		}
	}
	return false
}

// Resolve 按请求的 Bearer 密钥解析成员；无匹配返回 nil。
//
// 遍历全部成员且不做提前返回，使比较耗时不随"命中位置"变化；
// 单次比较本身是常量时间的（sha256 摘要 + ConstantTimeCompare）。
func (s *Store) Resolve(r *http.Request) *Member {
	tok, ok := httpauth.BearerToken(r)
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var hit *Member
	for _, m := range s.members {
		if httpauth.KeyEqual(tok, m.Key) {
			hit = m
		}
	}
	return hit
}

// Record 记账一次请求的消耗。
//   - credit 为本次 usage.credit（0 表示极小请求或上游未返回）；
//   - ok=false 表示该请求最终失败（只计 Errors，不加 credit）。
//
// 请求数在任何结局下都累加，便于观察"某成员的失败率"。
func (s *Store) Record(id string, credit float64, ok bool) {
	if id == "" {
		return
	}
	now := time.Now()
	cur := now.Unix() / bucketSec

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.members {
		if m.ID != id {
			continue
		}
		m.Requests++
		m.LastUsed = now.Unix()
		if !ok {
			m.Errors++
		} else if credit > 0 {
			m.TotalCredit += credit
			addBucket(m, cur, credit)
		}
		s.dirty = true
		return
	}
}

// addBucket 把消耗累加进当前时间桶，并裁剪超期桶。
func addBucket(m *Member, cur int64, credit float64) {
	for i := range m.Buckets {
		if m.Buckets[i].H == cur {
			m.Buckets[i].C += credit
			return
		}
	}
	m.Buckets = append(m.Buckets, bucket{H: cur, C: credit})
	// 桶按时间递增（除首次追加外），裁剪最老的。
	if len(m.Buckets) > bucketKeep {
		m.Buckets = m.Buckets[len(m.Buckets)-bucketKeep:]
	}
}

// window 计算最近 hours 小时内的消耗：累加时间桶。
// 粒度 15 分钟，故实际跨度为 (hours-0.25h, hours]。
func (m *Member) window(hours int, now int64) float64 {
	cur := now / bucketSec
	min := cur - int64(hours)*3600/bucketSec + 1
	var sum float64
	for _, b := range m.Buckets {
		if b.H >= min {
			sum += b.C
		}
	}
	return sum
}

// View 面板展示用的成员视图（含窗口用量与脱敏密钥）。
type View struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Note        string  `json:"note,omitempty"`
	Key         string  `json:"key"`
	KeyMasked   string  `json:"key_masked"`
	KeyHint     string  `json:"key_hint"` // 末 6 位，便于对号
	Created     string  `json:"created"`
	LastUsed    string  `json:"last_used,omitempty"`
	TotalCredit float64 `json:"total_credit"`
	Requests    int64   `json:"requests"`
	Errors      int64   `json:"errors"`
	Window5h    float64 `json:"window_5h"`
	Window24h   float64 `json:"window_24h"`
}

// List 返回全部成员视图（按消耗降序，未使用的排在后面）。
func (s *Store) List() []View {
	now := time.Now().Unix()
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]View, 0, len(s.members))
	for _, m := range s.members {
		v := View{
			ID:          m.ID,
			Name:        m.Name,
			Note:        m.Note,
			Key:         m.Key,
			KeyMasked:   maskKey(m.Key),
			KeyHint:     keyHint(m.Key),
			Created:     time.Unix(m.Created, 0).Format(time.RFC3339),
			TotalCredit: round2(m.TotalCredit),
			Requests:    m.Requests,
			Errors:      m.Errors,
			Window5h:    round2(m.window(5, now)),
			Window24h:   round2(m.window(24, now)),
		}
		if m.LastUsed > 0 {
			v.LastUsed = time.Unix(m.LastUsed, 0).Format(time.RFC3339)
		}
		out = append(out, v)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].TotalCredit != out[j].TotalCredit {
			return out[i].TotalCredit > out[j].TotalCredit
		}
		return out[i].Created < out[j].Created
	})
	return out
}

// Summary 成员维度汇总。
type Summary struct {
	Count       int     `json:"count"`
	TotalCredit float64 `json:"total_credit"`
	Window5h    float64 `json:"window_5h"`
	Window24h   float64 `json:"window_24h"`
	Requests    int64   `json:"requests"`
	Error       string  `json:"error,omitempty"`
}

// Stats 汇总 + 明细。
func (s *Store) Stats() (Summary, []View) {
	list := s.List()
	var sum Summary
	sum.Count = len(list)
	for _, v := range list {
		sum.TotalCredit += v.TotalCredit
		sum.Window5h += v.Window5h
		sum.Window24h += v.Window24h
		sum.Requests += v.Requests
	}
	sum.TotalCredit = round2(sum.TotalCredit)
	sum.Window5h = round2(sum.Window5h)
	sum.Window24h = round2(sum.Window24h)
	s.mu.Lock()
	sum.Error = s.lastErr
	s.mu.Unlock()
	return sum, list
}

func round2(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

func maskKey(k string) string {
	if len(k) <= 10 {
		return strings.Repeat("•", len(k))
	}
	return k[:7] + strings.Repeat("•", 12) + k[len(k)-4:]
}

func keyHint(k string) string {
	if len(k) <= 6 {
		return k
	}
	return k[len(k)-6:]
}

// DefaultPath 由状态文件路径推导成员文件路径（同目录 members.json）。
// state_file 为空时回落 ./data/members.json。
func DefaultPath(stateFile string) string {
	if stateFile == "" {
		return filepath.Join("data", "members.json")
	}
	return filepath.Join(filepath.Dir(stateFile), "members.json")
}
