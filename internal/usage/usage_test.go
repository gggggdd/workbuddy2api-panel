package usage

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/pool"
	"github.com/linguo2625469/workbuddy2api-panel/internal/upstream"
)

func mkSample(t int64, accts map[string]int64) sample {
	return sample{T: t, Accounts: accts}
}

// 基础窗口计算：used 单调递增时，窗口值 = 当前 − 窗口起点基线。
func TestWindowBasic(t *testing.T) {
	now := int64(100000)
	samples := []sample{
		mkSample(now-24*3600, map[string]int64{"a": 100, "b": 50}),
		mkSample(now-5*3600, map[string]int64{"a": 200, "b": 80}),
		mkSample(now, map[string]int64{"a": 320, "b": 90}),
	}
	w5 := window(samples, 5*time.Hour)
	// 5h 窗口：a 320-200=120，b 90-80=10 → 130
	if w5.Used != 130 {
		t.Errorf("5h used=%d want 130", w5.Used)
	}
	if !w5.Complete {
		t.Errorf("5h should be complete (oldest sample covers window)")
	}
	w24 := window(samples, 24*time.Hour)
	// 24h 窗口：a 320-100=220，b 90-50=40 → 260
	if w24.Used != 260 {
		t.Errorf("24h used=%d want 260", w24.Used)
	}
}

// 历史不足时 Complete=false 且 Used 以现有最老样本为基线（不虚报）。
func TestWindowIncomplete(t *testing.T) {
	now := int64(100000)
	samples := []sample{
		mkSample(now-600, map[string]int64{"a": 100}), // 仅 10 分钟历史
		mkSample(now, map[string]int64{"a": 140}),
	}
	w := window(samples, 24*time.Hour)
	if w.Complete {
		t.Errorf("should be incomplete with only 10min of history")
	}
	if w.Used != 40 {
		t.Errorf("used=%d want 40 (baseline = oldest available)", w.Used)
	}
	if w.Seconds != 600 {
		t.Errorf("seconds=%d want 600", w.Seconds)
	}
}

// 窗口内新增的账号没有基线 → 不计入；已移除的账号不再出现 → 计 0。
// 这是逐账号比对（而非比总量）的核心价值。
func TestWindowAccountChurn(t *testing.T) {
	now := int64(100000)
	samples := []sample{
		mkSample(now-24*3600, map[string]int64{"a": 100, "gone": 500}),
		mkSample(now, map[string]int64{"a": 150, "new": 9000}),
	}
	w := window(samples, 24*time.Hour)
	// a: 50；gone 不在当前样本 → 不计；new 无基线 → 不计
	if w.Used != 50 {
		t.Errorf("used=%d want 50 (churn must not inflate)", w.Used)
	}
}

// 套餐周期重置导致 used 归零 → 负增量钳 0（宁可少算，不虚报）。
func TestWindowNegativeClamped(t *testing.T) {
	now := int64(100000)
	samples := []sample{
		mkSample(now-3600, map[string]int64{"a": 5000}),
		mkSample(now, map[string]int64{"a": 10}), // 重置后
	}
	w := window(samples, 24*time.Hour)
	if w.Used != 0 {
		t.Errorf("used=%d want 0 (negative delta clamped)", w.Used)
	}
}

// 采样失败时沿用上次已知值：避免窗口内消耗被静默漏算。
// 用一个会在第 2 次查询失败的假上游验证序列不出现"骤降/缺失"。
func TestSampleNowCarriesForwardOnError(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 2 {
			w.WriteHeader(500) // 第 2 次采样失败
			return
		}
		// used 随调用递增：100 → (失败) → 140
		used := 100
		if n >= 3 {
			used = 140
		}
		fmt.Fprintf(w, `{"code":0,"data":{"Response":{"Data":{"Accounts":[{"CycleCapacitySize":1000,"CycleCapacityRemain":%d,"CycleCapacityUsed":%d}]}}}}`, 1000-used, used)
	}))
	defer srv.Close()

	up := upstream.New()
	up.BillingBaseCN = srv.URL // 指向假上游
	p := pool.New("")
	a := &auth.Auth{AccessToken: "at", UID: "u1"}
	p.Add(a) // 加入池（SyncToDir 的等价单账号入口）

	tr := New(p, up, "") // 不持久化
	tr.SampleNow()
	tr.SampleNow() // 失败
	tr.SampleNow()

	st := tr.Stats()
	if len(tr.samples) != 2 {
		t.Fatalf("samples=%d want 2 (failed round must not append)", len(tr.samples))
	}
	// 失败那轮沿用了 100，故 3 轮后是 140-100=40，而不是因缺失被漏算。
	if st.Window24h.Used != 40 {
		t.Errorf("used=%d want 40 (carry-forward keeps window intact)", st.Window24h.Used)
	}
}

// 全部账号查询失败时不追加样本（不写全零假样本）。
func TestSampleNowAllFailNoSample(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	up := upstream.New()
	up.BillingBaseCN = srv.URL
	p := pool.New("")
	p.Add(&auth.Auth{AccessToken: "at", UID: "u1"})

	tr := New(p, up, "")
	tr.SampleNow()
	if len(tr.samples) != 0 {
		t.Errorf("samples=%d want 0 (all failed)", len(tr.samples))
	}
}

// trim 丢弃超期样本并强制硬上限。
func TestTrim(t *testing.T) {
	now := time.Unix(1000000, 0)
	var s []sample
	for i := 0; i < 10; i++ {
		s = append(s, mkSample(now.Add(-time.Duration(i)*time.Hour).Unix(), map[string]int64{"a": int64(i)}))
	}
	// 保留 48h 内：全部保留
	if got := trim(s, now); len(got) != 10 {
		t.Errorf("trim kept %d want 10", len(got))
	}
	// 超期：3 天前的样本被丢弃
	old := append([]sample{}, s...)
	old = append(old, mkSample(now.Add(-72*time.Hour).Unix(), map[string]int64{"a": 99}))
	if got := trim(old, now); len(got) != 10 {
		t.Errorf("trim kept %d want 10 (expired dropped)", len(got))
	}
}

// DefaultPath 由 state_file 推导同目录 usage.json。
func TestDefaultPath(t *testing.T) {
	if got := DefaultPath("/app/data/state.json"); got != "/app/data/usage.json" && got != `\app\data\usage.json` {
		t.Errorf("DefaultPath=%q", got)
	}
	if got := DefaultPath(""); got == "" {
		t.Error("DefaultPath empty should fall back to data/usage.json")
	}
}
