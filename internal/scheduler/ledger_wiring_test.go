package scheduler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"workbuddy2api/internal/auth"
	"workbuddy2api/internal/ledger"
	"workbuddy2api/internal/pool"
	"workbuddy2api/internal/upstream"
)

// checkinWithCreditServer 上游：签到返回 credit=9，余额查询返回可解析结构。
// 用于验证「签到 → 账本入账」这条 scheduler 侧链路（Ledger 由 main 注入）。
func checkinWithCreditServer(t *testing.T) (*httptest.Server, *upstream.Client) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/daily-checkin"):
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0, "msg": "ok",
				"data": map[string]any{"Response": map[string]any{"Data": map[string]any{
					"credit": 9, "total_credit": 500,
				}}},
			})
		case strings.HasSuffix(r.URL.Path, "/get-user-resource"):
			w.Write([]byte(`{"code":0,"data":{"Response":{"Data":{"Accounts":[{"CycleCapacitySize":100,"CycleCapacityRemain":500,"CycleCapacityUsed":0}]}}}}`))
		default:
			http.Error(w, "not found", 404)
		}
	}))
	up := &upstream.Client{HTTP: srv.Client(), ChatBaseCN: srv.URL, BillingBaseCN: srv.URL}
	return srv, up
}

// TestRunCheckinRecordsLedger 签到成功后账本记一笔 checkin 入账（正数）。
//
// 回归保护：2026-09-15 的整文件覆盖把 cmd/server/main.go 里的
// scheduler.Config{Ledger: lgr} 一起删掉了，导致 s.lg() 恒为 nil，
// 所有定时入账（签到/旅行/连登）静默失效。此测试固定住 scheduler 侧的
// Ledger 消费路径：只要 Config.Ledger 被接上就必须入账。
func TestRunCheckinRecordsLedger(t *testing.T) {
	srv, up := checkinWithCreditServer(t)
	defer srv.Close()

	p := pool.New("")
	p.Add(&auth.Auth{UID: "u1", AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999})

	lgr := ledger.New("", 100)
	s := New(Config{Pool: p, Upstream: up, Ledger: lgr})
	s.RunCheckinNow()

	entries := lgr.List(ledger.Query{Kind: ledger.KindCheckin})
	if len(entries) != 1 {
		t.Fatalf("checkin ledger entries=%d want 1 (Ledger not wired?)", len(entries))
	}
	e := entries[0]
	if e.UID != "u1" || e.Delta != 9 {
		t.Errorf("entry=%+v want uid=u1 delta=9", e)
	}
	if e.Note != "每日签到" {
		t.Errorf("note=%q", e.Note)
	}
}

// TestRunCheckinWithoutLedgerDoesNotPanic Ledger 未注入（nil）时签到照常执行、不入账、不 panic。
func TestRunCheckinWithoutLedgerDoesNotPanic(t *testing.T) {
	srv, up := checkinWithCreditServer(t)
	defer srv.Close()

	p := pool.New("")
	p.Add(&auth.Auth{UID: "u1", AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999})

	s := New(Config{Pool: p, Upstream: up}) // 无 Ledger
	s.RunCheckinNow()                       // 不应 panic

	if st, ok := p.Status("u1"); !ok || st.Credits != 500 {
		t.Errorf("checkin should still refresh credits, got %+v", st)
	}
}

// TestRunCheckinSkipsGlobalRealmForLedger global 账号无 CN 签到体系 → 不产生任何账本条目。
func TestRunCheckinSkipsGlobalRealmForLedger(t *testing.T) {
	srv, up := checkinWithCreditServer(t)
	defer srv.Close()

	p := pool.New("")
	a := &auth.Auth{UID: "g1", AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999}
	p.Add(a)
	_, _ = auth.BackfillRealmFor(a, "global")

	lgr := ledger.New("", 100)
	s := New(Config{Pool: p, Upstream: up, Ledger: lgr})
	s.RunCheckinNow() // 内部 IsGlobal() 门控

	if n := len(lgr.List(ledger.Query{})); n != 0 {
		t.Errorf("global account must not produce ledger entries, got %d", n)
	}
	_ = time.Now
}
