package ledger

import (
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

func TestRollingCap(t *testing.T) {
	s := New("", 10)
	for i := 0; i < 100; i++ {
		s.Append(Entry{UID: "a", Kind: KindChat, Delta: -1})
	}
	if n := len(s.List(Query{})); n > 10 {
		t.Fatalf("cap not enforced: %d", n)
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
