package panel

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// TestSWRFirstFetchSync 首次访问同步拉取，之后命中缓存。
func TestSWRFirstFetchSync(t *testing.T) {
	calls := 0
	c := newSWR(time.Minute, func() (any, error) {
		calls++
		return "v1", nil
	})
	v, _, cached := c.get(false)
	if v != "v1" || cached {
		t.Fatalf("first get: v=%v cached=%v", v, cached)
	}
	v, _, cached = c.get(false)
	if v != "v1" || !cached {
		t.Fatalf("second get should hit cache: v=%v cached=%v", v, cached)
	}
	if calls != 1 {
		t.Fatalf("fetch called %d times, want 1", calls)
	}
}

// TestSWRStaleTriggersBackgroundRefresh 数据过期后立即返回旧值，后台刷新供下次用。
func TestSWRStaleTriggersBackgroundRefresh(t *testing.T) {
	n := 0
	var mu sync.Mutex
	c := newSWR(10*time.Millisecond, func() (any, error) {
		mu.Lock()
		defer mu.Unlock()
		// 拉取放慢，确保断言时后台刷新尚未完成（v2 仍取到旧值 v1）。
		time.Sleep(80 * time.Millisecond)
		n++
		return n, nil
	})
	v1, _, _ := c.get(false)
	time.Sleep(20 * time.Millisecond) // 过期
	v2, _, cached := c.get(false)
	if !cached {
		t.Fatal("stale get should report cached=true (serving stale)")
	}
	if v1 != v2 {
		t.Fatalf("stale serve should return old value first: v1=%v v2=%v", v1, v2)
	}
	// 等后台刷新完成，下次拿到新值。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		v3, _, _ := c.get(false)
		if v3 != v1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("background refresh did not produce a new value in time")
}

// TestSWRSingleflight 并发命中过期时只刷一次。
func TestSWRSingleflight(t *testing.T) {
	var mu sync.Mutex
	n := 0
	c := newSWR(10*time.Millisecond, func() (any, error) {
		mu.Lock()
		n++
		mu.Unlock()
		time.Sleep(50 * time.Millisecond) // 拉取慢，放大并发窗口
		return n, nil
	})
	c.get(false)
	time.Sleep(20 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.get(false)
		}()
	}
	wg.Wait()
	time.Sleep(150 * time.Millisecond) // 等可能的重复刷新落账
	mu.Lock()
	defer mu.Unlock()
	if n > 3 { // 1 首次 + 1 后台刷新（singleflight），留裕量
		t.Fatalf("fetch fired %d times, singleflight should cap near 2", n)
	}
}

// TestSWRForceSyncRefresh force=1 同步绕过缓存，立即拿到新值。
func TestSWRForceSyncRefresh(t *testing.T) {
	n := 0
	c := newSWR(time.Hour, func() (any, error) {
		n++
		return n, nil
	})
	c.get(false)
	v, _, cached := c.get(true)
	if cached {
		t.Fatal("force get should not report cached")
	}
	if v != 2 {
		t.Fatalf("force get should fetch fresh: v=%v", v)
	}
}

// TestSWRFetchFailureKeepsOldValue 刷新失败时保留旧数据（宁旧勿空）。
func TestSWRFetchFailureKeepsOldValue(t *testing.T) {
	fail := false
	c := newSWR(10*time.Millisecond, func() (any, error) {
		if fail {
			return nil, errors.New("boom")
		}
		return "good", nil
	})
	c.get(false)
	time.Sleep(20 * time.Millisecond)
	fail = true
	v, _, _ := c.get(false)
	if v != "good" {
		t.Fatalf("failed refresh should keep old value, got %v", v)
	}
}
