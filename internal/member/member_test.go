package member

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func reqWithKey(key string) *http.Request {
	r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	if key != "" {
		r.Header.Set("Authorization", "Bearer "+key)
	}
	return r
}

// 新建成员应签发非空且唯一的密钥。
func TestAddGeneratesKey(t *testing.T) {
	s := NewStore("")
	a := s.Add("张三", "")
	b := s.Add("李四", "")
	if a.Key == "" || b.Key == "" {
		t.Fatal("key must not be empty")
	}
	if a.Key == b.Key {
		t.Error("keys must be unique")
	}
	if a.ID == b.ID {
		t.Error("ids must be unique")
	}
	// 空名回落默认
	if c := s.Add("   ", ""); c.Name != "成员" {
		t.Errorf("blank name should fall back, got %q", c.Name)
	}
}

// Resolve 按密钥匹配到正确成员；未知密钥/缺失头返回 nil。
func TestResolve(t *testing.T) {
	s := NewStore("")
	a := s.Add("甲", "")
	b := s.Add("乙", "")

	if got := s.Resolve(reqWithKey(a.Key)); got == nil || got.ID != a.ID {
		t.Errorf("resolve a failed: %+v", got)
	}
	if got := s.Resolve(reqWithKey(b.Key)); got == nil || got.ID != b.ID {
		t.Errorf("resolve b failed: %+v", got)
	}
	if got := s.Resolve(reqWithKey("wb-wrong")); got != nil {
		t.Errorf("unknown key must not resolve, got %+v", got)
	}
	if got := s.Resolve(reqWithKey("")); got != nil {
		t.Errorf("missing header must not resolve")
	}
	// 管理员密钥（任意非成员值）不应命中任何成员
	if got := s.Resolve(reqWithKey("test_key")); got != nil {
		t.Errorf("admin key must not resolve as member")
	}
}

// Record 把消耗累加进窗口与总量；失败只计错误不加消耗。
func TestRecord(t *testing.T) {
	s := NewStore("")
	m := s.Add("甲", "")

	s.Record(m.ID, 1.5, true)
	s.Record(m.ID, 0.25, true)
	s.Record(m.ID, 0, false) // 失败：不加消耗

	_, list := s.Stats()
	if len(list) != 1 {
		t.Fatalf("want 1 member, got %d", len(list))
	}
	v := list[0]
	if v.Requests != 3 {
		t.Errorf("requests=%d want 3", v.Requests)
	}
	if v.Errors != 1 {
		t.Errorf("errors=%d want 1", v.Errors)
	}
	if v.TotalCredit != 1.75 {
		t.Errorf("total=%v want 1.75", v.TotalCredit)
	}
	if v.Window5h != 1.75 || v.Window24h != 1.75 {
		t.Errorf("windows should include fresh usage: 5h=%v 24h=%v", v.Window5h, v.Window24h)
	}
	if v.LastUsed == "" {
		t.Error("LastUsed should be set")
	}
}

// 零消耗（credit=0）的成功请求仍计请求数，但不进桶。
func TestRecordZeroCredit(t *testing.T) {
	s := NewStore("")
	m := s.Add("甲", "")
	s.Record(m.ID, 0, true)
	_, list := s.Stats()
	if list[0].Requests != 1 || list[0].TotalCredit != 0 {
		t.Errorf("want requests=1 total=0, got %+v", list[0])
	}
}

// 窗口外（25 小时前）的桶不计入 5h/24h，但仍留在总量里。
func TestWindowExcludesOldBuckets(t *testing.T) {
	s := NewStore("")
	m := s.Add("甲", "")
	s.Record(m.ID, 10, true)

	// 手工把桶挪到 25 小时前
	s.mu.Lock()
	old := time.Now().Unix()/bucketSec - 24*3600/bucketSec - 5
	s.members[0].Buckets = []bucket{{H: old, C: 10}}
	s.mu.Unlock()

	_, list := s.Stats()
	if list[0].Window5h != 0 || list[0].Window24h != 0 {
		t.Errorf("old bucket must be excluded: 5h=%v 24h=%v", list[0].Window5h, list[0].Window24h)
	}
	if list[0].TotalCredit != 10 {
		t.Errorf("total must keep historical usage: %v", list[0].TotalCredit)
	}
}

// 桶数量受上限约束（不会无限增长）。
func TestBucketCap(t *testing.T) {
	s := NewStore("")
	m := s.Add("甲", "")
	s.mu.Lock()
	for i := 0; i < 500; i++ {
		addBucket(s.members[0], int64(i), 1)
	}
	n := len(s.members[0].Buckets)
	s.mu.Unlock()
	if n > bucketKeep {
		t.Errorf("buckets=%d must be capped at %d", n, bucketKeep)
	}
	_ = m
}

// Rotate 换密钥后旧密钥立即失效。
func TestRotateInvalidatesOldKey(t *testing.T) {
	s := NewStore("")
	m := s.Add("甲", "")
	old := m.Key

	newKey, ok := s.Rotate(m.ID)
	if !ok || newKey == "" || newKey == old {
		t.Fatalf("rotate failed: ok=%v new=%q", ok, newKey)
	}
	if got := s.Resolve(reqWithKey(old)); got != nil {
		t.Error("old key must stop working after rotate")
	}
	if got := s.Resolve(reqWithKey(newKey)); got == nil {
		t.Error("new key must work")
	}
}

// ResetUsage 清计数但保留成员与密钥。
func TestResetUsage(t *testing.T) {
	s := NewStore("")
	m := s.Add("甲", "")
	s.Record(m.ID, 5, true)
	if !s.ResetUsage(m.ID) {
		t.Fatal("reset failed")
	}
	_, list := s.Stats()
	if list[0].TotalCredit != 0 || list[0].Requests != 0 {
		t.Errorf("usage should be cleared: %+v", list[0])
	}
	if list[0].Key != m.Key {
		t.Error("key must survive reset")
	}
}

// Update 只改提供的字段（空名不覆盖）。
func TestUpdate(t *testing.T) {
	s := NewStore("")
	m := s.Add("甲", "原备注")
	s.Update(m.ID, "", "新备注")
	_, list := s.Stats()
	if list[0].Name != "甲" {
		t.Errorf("empty name must not overwrite, got %q", list[0].Name)
	}
	if list[0].Note != "新备注" {
		t.Errorf("note=%q want 新备注", list[0].Note)
	}
	s.Update(m.ID, "甲改", "")
	if _, l := s.Stats(); l[0].Name != "甲改" {
		t.Errorf("name should update")
	}
}

// Remove 删除后密钥失效。
func TestRemove(t *testing.T) {
	s := NewStore("")
	m := s.Add("甲", "")
	if !s.Remove(m.ID) {
		t.Fatal("remove failed")
	}
	if s.Count() != 0 {
		t.Error("member should be gone")
	}
	if got := s.Resolve(reqWithKey(m.Key)); got != nil {
		t.Error("removed member's key must not resolve")
	}
	if s.Remove("nonexistent") {
		t.Error("removing unknown id should return false")
	}
}

// 持久化：落盘后重新加载，成员与用量保留。
func TestPersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "members.json")

	s := NewStore(path)
	m := s.Add("甲", "备注")
	s.Record(m.ID, 2.5, true)
	s.Flush()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file not written: %v", err)
	}

	s2 := NewStore(path)
	if s2.Count() != 1 {
		t.Fatalf("reload count=%d want 1", s2.Count())
	}
	_, list := s2.Stats()
	if list[0].Name != "甲" || list[0].TotalCredit != 2.5 || list[0].Key != m.Key {
		t.Errorf("reload mismatch: %+v", list[0])
	}
	// 重载后密钥仍可用
	if got := s2.Resolve(reqWithKey(m.Key)); got == nil {
		t.Error("reloaded key must resolve")
	}
}

// 损坏的文件不应导致 panic，按空集合启动。
func TestLoadCorrupt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "members.json")
	os.WriteFile(path, []byte("{not json"), 0o600)

	s := NewStore(path)
	if s.Count() != 0 {
		t.Errorf("corrupt file should yield empty store, got %d", s.Count())
	}
	// 仍可正常新增
	if m := s.Add("甲", ""); m.Key == "" {
		t.Error("add after corrupt load should work")
	}
}

// 密钥掩码不泄露完整密钥。
func TestMaskedKeyHidesSecret(t *testing.T) {
	s := NewStore("")
	m := s.Add("甲", "")
	_, list := s.Stats()
	if list[0].KeyMasked == m.Key {
		t.Error("masked must differ from raw key")
	}
	if len(list[0].KeyHint) > 6 {
		t.Errorf("hint too long: %q", list[0].KeyHint)
	}
	if list[0].Key != m.Key {
		t.Error("View.Key should carry raw key for admin copy")
	}
}
