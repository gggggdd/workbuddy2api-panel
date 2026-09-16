package ledger

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAppendAndList(t *testing.T) {
	s := New("", 100) // 纯内存
	s.Append(Entry{UID: "a", Nick: "甲", Kind: KindCheckin, Delta: 100})
	s.Append(Entry{UID: "a", Kind: KindChat, Delta: -12.5, Model: "glm-5.3", Member: "m1"})
	s.Append(Entry{UID: "b", Kind: KindTravel, Delta: 30})
	s.Append(Entry{UID: "a", Kind: KindChat, Delta: 0}) // 零变动应被忽略

	got := s.List(Query{UID: "a"})
	if len(got) != 2 {
		t.Fatalf("want 2 entries for uid a, got %d", len(got))
	}
	if got[0].Kind != KindChat || got[0].Delta != -12.5 {
		t.Fatalf("list should be desc by time: %+v", got[0])
	}
	if got[0].At.IsZero() {
		t.Fatal("Append should stamp time")
	}
}

func TestSummarize(t *testing.T) {
	s := New("", 100)
	now := time.Now()
	s.Append(Entry{At: now.Add(-2 * time.Hour), UID: "a", Nick: "甲", Kind: KindCheckin, Delta: 100})
	s.Append(Entry{At: now.Add(-1 * time.Hour), UID: "a", Kind: KindChat, Delta: -40})
	s.Append(Entry{At: now.Add(-30 * time.Minute), UID: "a", Kind: KindChat, Delta: -10, Member: "m1"})
	s.Append(Entry{At: now.Add(-48 * time.Hour), UID: "a", Kind: KindGift, Delta: 500}) // 范围外

	sums := s.Summarize(Query{Since: now.Add(-24 * time.Hour)})
	if len(sums) != 1 {
		t.Fatalf("want 1 account, got %d", len(sums))
	}
	a := sums[0]
	if a.Inflow != 100 || a.Outflow != 50 || a.Net != 50 || a.ChatCount != 2 {
		t.Fatalf("summary mismatch: %+v", a)
	}

	in, out := s.Totals(Query{Since: now.Add(-24 * time.Hour)})
	if in != 100 || out != 50 {
		t.Fatalf("totals mismatch: in=%v out=%v", in, out)
	}
}

// TestRollingCap 溢出裁剪必须「保留最新 max 条」。
//
// 回归保护：原实现按 n-cut 起点裁剪（cut=max/8），保留条数只有 cut，
// 上限一调到会被触及的量级就把账本砸到 1/8（实测 1000 → 125）。
//
// 断言用「条数 == max」+「留下的就是最后写入的那批」两条：
// 只断言 n <= max 会漏掉过度裁剪，且追加次数必须避开与裁剪周期的整数倍
// 巧合（buggy 版会在 cut..max 间周期震荡，取巧的次数可能恰好落在 max）。
func TestRollingCap(t *testing.T) {
	const max = 10
	const n = max*7 + 3 // 远离周期整数倍，避免震荡恰好回到 max
	s := New("", max)
	for i := 1; i <= n; i++ {
		s.Append(Entry{UID: "a", Kind: KindChat, Delta: -float64(i)}) // Delta 单调，可辨识新旧
	}
	got := s.List(Query{})
	if len(got) != max {
		t.Fatalf("kept %d, want exactly %d（少了=过度裁剪，多了=未裁剪）", len(got), max)
	}
	// 保留的必须是最新的 max 条：List 时间倒序，out[0] 即最后写入的 -n。
	if got[0].Delta != -float64(n) {
		t.Errorf("newest kept=%v want %v（留下了旧条目）", got[0].Delta, -float64(n))
	}
	if last := got[max-1].Delta; last != -float64(n-max+1) {
		t.Errorf("oldest kept=%v want %v（保留窗口不对）", last, -float64(n-max+1))
	}
}

func TestPersistRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	s := New(path, 100)
	s.Append(Entry{UID: "a", Kind: KindCheckin, Delta: 88})
	s.Append(Entry{UID: "b", Kind: KindChat, Delta: -7, Model: "auto"})
	time.Sleep(1200 * time.Millisecond) // 等去抖落盘

	s2 := New(path, 100)
	got := s2.List(Query{})
	if len(got) != 2 {
		t.Fatalf("want 2 after reload, got %d", len(got))
	}
	if got[1].Delta != 88 {
		t.Fatalf("order/content mismatch: %+v", got[1])
	}
}

// TestLoadTrimsToCap 上限收缩后重启：旧文件里的超额条目按同一规则截断，
// 只留最新 max 条（否则调小上限无效，旧数据要等新记录逐条挤出去）。
func TestLoadTrimsToCap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	now := time.Now()
	var st snapshot
	for i := 0; i < 2500; i++ { // 模拟旧文件：按大上限攒下的 2500 条
		st.Entries = append(st.Entries, Entry{
			At: now.Add(time.Duration(i) * time.Second), UID: "a", Kind: KindChat, Delta: -1,
		})
	}
	raw, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	s := New(path, 1000) // 上限收缩
	got := s.List(Query{})
	if len(got) != 1000 {
		t.Fatalf("after reload len=%d want 1000（旧文件未按新上限截断）", len(got))
	}
	// 留下的应是最新的 1000 条（List 时间倒序，got[0] 即最新）。
	if want := now.Add(2499 * time.Second); !got[0].At.Equal(want) {
		t.Errorf("kept newest? got[0].At=%v want %v", got[0].At, want)
	}
	if want := now.Add(1500 * time.Second); !got[999].At.Equal(want) {
		t.Errorf("kept oldest of window? got[999].At=%v want %v", got[999].At, want)
	}
}

// TestTrimmedRefSurvivesRestart 被上限裁掉的条目，其幂等键必须跨重启保留：
// 否则下一轮上游同步（每次拉全量、可重复执行）会把已丢弃的奖励重新补回，
// 截断形同虚设，且这些旧奖励会以数组末尾位置冒到列表顶部。
//
// 直接构造落盘快照，避免依赖去抖落盘时序：模拟「task 条目已被裁掉、
// 只剩 chat 条目」的账本，但 Refs 里仍留有该 task 的幂等键。
func TestTrimmedRefSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	const ref = "school|a|t1|100|0"
	raw, err := json.Marshal(snapshot{
		Entries: []Entry{{UID: "a", Kind: KindChat, Delta: -1}}, // task 已溢出被裁
		Refs:    []string{ref},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	s := New(path, 10) // 重启
	if s.AppendOnce(Entry{UID: "a", Kind: KindTask, Delta: 100, Ref: ref}) {
		t.Fatal("重放被裁掉的记录又被入账了（Ref 未跨重启保留，截断失效）")
	}
	if n := len(s.List(Query{Kind: KindTask})); n != 0 {
		t.Errorf("task entries=%d want 0", n)
	}
}

// TestTrimmedRefKeptInMemory 内存中溢出裁剪同样不得丢弃幂等键：
// 同一进程内后续同步（开学季每轮拉全量）不能把裁掉的奖励补回来。
func TestTrimmedRefKeptInMemory(t *testing.T) {
	s := New("", 10) // 纯内存，无落盘时序干扰
	const ref = "school|a|t1|100|0"
	if !s.AppendOnce(Entry{UID: "a", Kind: KindTask, Delta: 100, Ref: ref}) {
		t.Fatal("first append should be recorded")
	}
	for i := 0; i < 30; i++ { // 溢出，把 task 挤出窗口
		s.Append(Entry{UID: "a", Kind: KindChat, Delta: -1})
	}
	if n := len(s.List(Query{Kind: KindTask})); n != 0 {
		t.Fatalf("setup: task should be trimmed, got %d", n)
	}
	if s.AppendOnce(Entry{UID: "a", Kind: KindTask, Delta: 100, Ref: ref}) {
		t.Fatal("同进程内重放被裁掉的记录又被入账了")
	}
}
