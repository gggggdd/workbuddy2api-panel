package pool

import (
	"testing"
	"time"

	"workbuddy2api/internal/auth"
)

// TestExpiringClampedToCredits expiring 超过总量/负值时被钳制,不污染快照。
func TestExpiringClampedToCredits(t *testing.T) {
	p := New("")
	p.Add(&auth.Auth{UID: "u1"})
	p.SetCreditsDetailed("u1", 100, 100, 9999, time.Time{}, 0) // expiring > credits
	p.mu.RLock()
	if p.byUID["u1"].creditsExpiring != 100 {
		t.Errorf("creditsExpiring=%d want 100 (clamped to credits)", p.byUID["u1"].creditsExpiring)
	}
	p.mu.RUnlock()

	p.SetCreditsDetailed("u1", 100, 100, -5, time.Time{}, 0) // 负值
	p.mu.RLock()
	if p.byUID["u1"].creditsExpiring != 0 {
		t.Errorf("creditsExpiring=%d want 0 (negative clamped)", p.byUID["u1"].creditsExpiring)
	}
	p.mu.RUnlock()
}

// TestSetCreditsLeavesExpiringUnchanged 旧 SetCredits 只更新总量,不清 expiring(向后兼容)。
func TestSetCreditsLeavesExpiringUnchanged(t *testing.T) {
	p := New("")
	p.Add(&auth.Auth{UID: "u1"})
	p.SetCreditsDetailed("u1", 500, 500, 200, time.Now().Add(time.Hour), 200)
	p.SetCredits("u1", 600, 600) // 旧入口只更新总量
	p.mu.RLock()
	e := p.byUID["u1"]
	if e.credits != 600 {
		t.Errorf("credits=%d want 600", e.credits)
	}
	if e.creditsExpiring != 200 {
		t.Errorf("creditsExpiring=%d want 200 (SetCredits 不应清)", e.creditsExpiring)
	}
	p.mu.RUnlock()
}

// TestEarliestExpirySnapshotClears 最早到期批次在零值/过期时刻被清空(上游 1.11.8 口径)。
func TestEarliestExpirySnapshotClears(t *testing.T) {
	p := New("")
	p.Add(&auth.Auth{UID: "u1"})
	p.SetCreditsDetailed("u1", 100, 100, 50, time.Now().Add(time.Hour), 50)
	p.mu.RLock()
	if p.byUID["u1"].creditsEarliestRemaining != 50 {
		t.Errorf("earliestRemaining=%d want 50", p.byUID["u1"].creditsEarliestRemaining)
	}
	p.mu.RUnlock()

	// 过期时刻 → 快照清空
	p.SetCreditsDetailed("u1", 100, 100, 50, time.Now().Add(-time.Hour), 50)
	p.mu.RLock()
	if p.byUID["u1"].creditsEarliestRemaining != 0 || !p.byUID["u1"].creditsEarliestExpiry.IsZero() {
		t.Errorf("past expiry should clear snapshot, got remaining=%d expiry=%v",
			p.byUID["u1"].creditsEarliestRemaining, p.byUID["u1"].creditsEarliestExpiry)
	}
	p.mu.RUnlock()
}
