package scheduler

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"workbuddy2api/internal/auth"
	"workbuddy2api/internal/ledger"
	"workbuddy2api/internal/pool"
	"workbuddy2api/internal/upstream"
)

// 回归背景（2026-09-16）：积分明细里「只有消耗、没有来源」。
// 显示层原因（limit 截断）之外，记账侧实测缺口：
//   - scheduler/school.go 整个文件不引用 ledger：开学季任务奖励与转盘积分全丢
//     （日志实测 72h 内 33 笔抽奖 / 1038c 未入账）；
//   - streak.go 的连登抽奖循环只打日志；
//   - panel 手动签到只调 DailyCheckin（不入账），login 则连发两次 POST
//     导致第二次必返「今天已签到」、credit 恒 0。
//
// 本文件固定住「开学季任务/抽奖 → 账本」这条链路。

// schoolStub 开学季活动上游。权威积分记录端点 /rewards 是入账主路径；
// /tasks 给任务面额、/config 给转盘面额与抽奖次数、/wheel/draw 执行抽奖。
type schoolStub struct {
	// drawCredits 依次作为每次抽奖的 credit_amount（长度即抽奖次数）。
	drawCredits []int
	drawIdx     int
	// rewards 是不可变的权威记录快照（始终返回全量，模拟上游行为）。
	rewards []string // 每项形如 `{"type":"credit","granted_at":"...","amount":66}`
	// shareReward / shareStatus 控制 share_invite 条目。
	shareReward int
	shareStatus string
	// claimed 累计 claim 调用次数。
	claimed int
	// tasksCalls 记录 /tasks 轮询次数（share 会 poll）。
	tasksCalls int
	// rewardsCalls 记录 /rewards 拉取次数（验证入账走后端记录而非客户端埋点）。
	rewardsCalls int
}

func (s *schoolStub) server(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case strings.HasSuffix(p, "/portal/activity/school/rewards"):
			s.rewardsCalls++
			w.Write([]byte(`{"code":0,"message":"ok","data":{"has_more":false,"total":` +
				fmt.Sprint(len(s.rewards)) + `,"items":[` + strings.Join(s.rewards, ",") + `]}}`))
		case strings.HasSuffix(p, "/portal/activity/school/tasks"):
			s.tasksCalls++
			// 首次返回未完成，轮询后返回 completed（触发 share 的 poll → claim）。
			status := s.shareStatus
			if s.tasksCalls > 1 {
				status = "completed"
			}
			fmt.Fprintf(w, `{"code":0,"message":"ok","data":{"in_period":true,"tasks":[`+
				`{"task_code":"share_invite","status":%q,"progress":1,"target_count":1,"reward_credit":%d},`+
				`{"task_code":"chat_3_times","status":"claimed","progress":3,"target_count":3,"reward_credit":50},`+
				`{"task_code":"expert_use","status":"claimed","progress":1,"target_count":1,"reward_credit":50},`+
				`{"task_code":"desktop_chat_1_time","status":"claimed","progress":1,"target_count":1,"reward_credit":100}]}}`,
				status, s.shareReward)
		case strings.HasSuffix(p, "/tasks/share-complete"):
			w.Write([]byte(`{"code":0,"data":{}}`))
		case strings.HasSuffix(p, "/claim"):
			s.claimed++
			w.Write([]byte(`{"code":0,"data":{"chance_granted":1}}`))
		case strings.HasSuffix(p, "/portal/activity/school/config"):
			// 抽奖次数余额 = 剩余可抽次数（驱动 schoolAccount 的抽奖循环）；
			// prizes 提供转盘面额（6/66），供来源分类。
			bal := len(s.drawCredits) - s.drawIdx
			if bal < 0 {
				bal = 0
			}
			fmt.Fprintf(w, `{"code":0,"data":{"in_period":true,`+
				`"chance":{"balance":%d,"total_earned":%d},`+
				`"prizes":[{"prize_code":"school_credit_6","type":"credit","credit_amount":6},`+
				`{"prize_code":"school_credit_66","type":"credit","credit_amount":66},`+
				`{"prize_code":"school_voucher_kfc_ok","type":"voucher","credit_amount":0}]}}`,
				bal, len(s.drawCredits))
		case strings.HasSuffix(p, "/wheel/draw"):
			if s.drawIdx >= len(s.drawCredits) {
				w.WriteHeader(409)
				w.Write([]byte(`{"code":40900,"message":"no chance"}`))
				return
			}
			c := s.drawCredits[s.drawIdx]
			s.drawIdx++
			fmt.Fprintf(w, `{"code":0,"data":{"prize_code":"school_credit_%d","credit_amount":%d,"chance_balance":0}}`, c, c)
		case strings.HasSuffix(p, "/daily-checkin"):
			w.Write([]byte(`{"code":0,"data":{"Response":{"Data":{"credit":9,"total_credit":500}}}}`))
		case strings.HasSuffix(p, "/get-user-resource"):
			w.Write([]byte(`{"code":0,"data":{"Response":{"Data":{"Accounts":[{"CycleCapacitySize":100,"CycleCapacityRemain":500,"CycleCapacityUsed":0}]}}}}`))
		default:
			http.Error(w, "not found", 404)
		}
	}))
}

// newSchoolSched 构造 Pool+Upstream+Scheduler（含账本），账号 CN。
func newSchoolSched(t *testing.T, stub *schoolStub) (*Scheduler, *ledger.Store) {
	t.Helper()
	srv := stub.server(t)
	t.Cleanup(srv.Close)

	p := pool.New("")
	p.Add(&auth.Auth{UID: "u1", Nickname: "甲", AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999})
	lgr := ledger.New("", 500)
	up := &upstream.Client{HTTP: srv.Client(), ChatBaseCN: srv.URL, BillingBaseCN: srv.URL}
	return New(Config{Pool: p, Upstream: up, Ledger: lgr}), lgr
}

// TestSchoolRewardsRecordsLedger 按上游权威记录入账：转盘 66/6 记 lottery、
// 任务 100/50 记 task，金额与上游一致。
func TestSchoolRewardsRecordsLedger(t *testing.T) {
	fastActivity(t)
	stub := &schoolStub{
		shareReward: 100, shareStatus: "claimed",
		rewards: []string{
			`{"type":"credit","granted_at":"2026-09-16 00:09:20","amount":66}`,
			`{"type":"credit","granted_at":"2026-09-16 00:09:18","amount":6}`,
			`{"type":"credit","granted_at":"2026-09-16 00:09:02","amount":100}`,
			`{"type":"credit","granted_at":"2026-09-16 00:09:04","amount":50}`,
		},
	}
	s, lgr := newSchoolSched(t, stub)

	a := &auth.Auth{UID: "u1", Nickname: "甲", AccessToken: "at"}
	s.schoolAccount(a)

	lot := lgr.List(ledger.Query{Kind: ledger.KindLottery})
	if len(lot) != 2 {
		t.Fatalf("lottery entries=%d want 2（转盘积分未按上游记录入账）", len(lot))
	}
	tot := 0.0
	for _, e := range lot {
		tot += e.Delta
		if e.Note != "开学季转盘" || e.UID != "u1" {
			t.Errorf("lottery entry=%+v", e)
		}
	}
	if tot != 72 {
		t.Errorf("lottery total=%v want 72", tot)
	}

	task := lgr.List(ledger.Query{Kind: ledger.KindTask})
	if len(task) != 2 {
		t.Fatalf("task entries=%d want 2（任务积分未按上游记录入账）", len(task))
	}
	ttot := 0.0
	for _, e := range task {
		ttot += e.Delta
		if e.Note != "开学季任务" {
			t.Errorf("task entry=%+v", e)
		}
	}
	if ttot != 150 {
		t.Errorf("task total=%v want 150", ttot)
	}
}

// TestSchoolRewardsIdempotent 权威同步必须幂等：account 跑两轮（每轮拉两次 /rewards）
// 后账本条目数不增长——否则每次巡检都会重复记账。
func TestSchoolRewardsIdempotent(t *testing.T) {
	fastActivity(t)
	stub := &schoolStub{
		shareReward: 100, shareStatus: "claimed",
		rewards: []string{
			`{"type":"credit","granted_at":"2026-09-16 00:09:20","amount":66}`,
			`{"type":"credit","granted_at":"2026-09-16 00:09:02","amount":100}`,
		},
	}
	s, lgr := newSchoolSched(t, stub)

	a := &auth.Auth{UID: "u1", Nickname: "甲", AccessToken: "at"}
	s.schoolAccount(a)
	n1 := len(lgr.List(ledger.Query{}))
	s.schoolAccount(a) // 第二轮：同一条上游记录不得再入账
	n2 := len(lgr.List(ledger.Query{}))

	if n1 == 0 {
		t.Fatal("first round recorded nothing")
	}
	if n2 != n1 {
		t.Errorf("re-run grew ledger %d -> %d（幂等失效，会重复记账）", n1, n2)
	}
}

// TestSchoolRewardsDuplicateSameSecond 上游同一秒同额多笔（实测出现过 66+66）：
// 应各记一条，重复同步不增。
func TestSchoolRewardsDuplicateSameSecond(t *testing.T) {
	fastActivity(t)
	stub := &schoolStub{
		shareReward: 100, shareStatus: "claimed",
		rewards: []string{
			`{"type":"credit","granted_at":"2026-09-16 00:09:20","amount":66}`,
			`{"type":"credit","granted_at":"2026-09-16 00:09:20","amount":66}`,
		},
	}
	s, lgr := newSchoolSched(t, stub)

	a := &auth.Auth{UID: "u1", Nickname: "甲", AccessToken: "at"}
	s.schoolAccount(a)
	if n := len(lgr.List(ledger.Query{Kind: ledger.KindLottery})); n != 2 {
		t.Fatalf("entries=%d want 2（同秒同额两笔应各记一条）", n)
	}
	s.schoolAccount(a)
	if n := len(lgr.List(ledger.Query{Kind: ledger.KindLottery})); n != 2 {
		t.Errorf("entries=%d after re-run want 2", n)
	}
}

// TestSchoolRewardVoucherIgnored type=voucher 条目无积分，不入账。
func TestSchoolRewardVoucherIgnored(t *testing.T) {
	fastActivity(t)
	stub := &schoolStub{
		shareReward: 100, shareStatus: "claimed",
		rewards: []string{
			`{"type":"voucher","granted_at":"2026-09-16 00:09:20","amount":0}`,
		},
	}
	s, lgr := newSchoolSched(t, stub)

	a := &auth.Auth{UID: "u1", Nickname: "甲", AccessToken: "at"}
	s.schoolAccount(a)

	if n := len(lgr.List(ledger.Query{})); n != 0 {
		t.Errorf("voucher should not be recorded, got %d entries", n)
	}
}

// TestSchoolRewardUnparsableTimeStillRecorded 时间格式异常时仍入账（回落到当前时刻），
// 不因解析失败丢积分。
func TestSchoolRewardUnparsableTimeStillRecorded(t *testing.T) {
	fastActivity(t)
	stub := &schoolStub{
		shareReward: 100, shareStatus: "claimed",
		rewards: []string{`{"type":"credit","granted_at":"bogus","amount":66}`},
	}
	s, lgr := newSchoolSched(t, stub)

	a := &auth.Auth{UID: "u1", Nickname: "甲", AccessToken: "at"}
	s.schoolAccount(a)

	es := lgr.List(ledger.Query{Kind: ledger.KindLottery})
	if len(es) != 1 || es[0].Delta != 66 {
		t.Fatalf("entries=%+v want one 66 entry", es)
	}
}

// TestSchoolNoLedgerDoesNotPanic Ledger 未注入（nil）时活动照跑、不 panic。
func TestSchoolNoLedgerDoesNotPanic(t *testing.T) {
	fastActivity(t)
	stub := &schoolStub{
		shareReward: 100, shareStatus: "claimed",
		rewards:     []string{`{"type":"credit","granted_at":"2026-09-16 00:09:20","amount":66}`},
		drawCredits: []int{66},
	}
	srv := stub.server(t)
	defer srv.Close()

	p := pool.New("")
	p.Add(&auth.Auth{UID: "u1", AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999})
	up := &upstream.Client{HTTP: srv.Client(), ChatBaseCN: srv.URL, BillingBaseCN: srv.URL}
	s := New(Config{Pool: p, Upstream: up}) // 无 Ledger

	s.schoolAccount(&auth.Auth{UID: "u1", AccessToken: "at"}) // 不应 panic
}

// ---- 连登抽奖 ----

// TestStreakLotteryRecordsLedger 连登抽奖循环的积分入账。
// LotteryDraw 的载荷形状随活动期变化，用 prize 子对象形状验证宽松解析。
func TestStreakLotteryRecordsLedger(t *testing.T) {
	fastActivity(t)
	var draws int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/lottery/summary"):
			w.Write([]byte(`{"code":0,"data":{"chances":2,"module":{"enabled":true}}}`))
		case strings.HasSuffix(r.URL.Path, "/lottery/draw"):
			draws++
			amt, code := 66, "g_credit_66"
			if draws > 1 {
				amt, code = 6, "g_credit_6"
			}
			fmt.Fprintf(w, `{"code":0,"data":{"prize":{"prize_code":%q,"credit_amount":%d}}}`, code, amt)
		case strings.HasSuffix(r.URL.Path, "/activity/growth/streak"):
			w.Write([]byte(`{"code":0,"data":{"streak":{"days":1},"redemption_status":{"tiers":[]}}}`))
		case strings.HasSuffix(r.URL.Path, "/heatmap"):
			w.Write([]byte(`{"code":0,"data":{"cells":[]}}`))
		case strings.HasSuffix(r.URL.Path, "/claim-compensation"):
			w.Write([]byte(`{"code":10001,"msg":"no compensation"}`))
		case strings.HasSuffix(r.URL.Path, "/claim-gift"):
			w.Write([]byte(`{"code":10001,"msg":"no gift"}`))
		default:
			http.Error(w, "not found", 404)
		}
	}))
	defer srv.Close()

	p := pool.New("")
	p.Add(&auth.Auth{UID: "u1", Nickname: "甲", AccessToken: "at", RefreshToken: "rt", ExpiresAt: 9999999999})
	lgr := ledger.New("", 500)
	up := &upstream.Client{HTTP: srv.Client(), ChatBaseCN: srv.URL, BillingBaseCN: srv.URL}
	s := New(Config{Pool: p, Upstream: up, Ledger: lgr})

	a := &auth.Auth{UID: "u1", Nickname: "甲", AccessToken: "at"}
	s.streakBonusAccount(a)

	entries := lgr.List(ledger.Query{Kind: ledger.KindLottery})
	if len(entries) != 2 {
		t.Fatalf("lottery entries=%d want 2（连登抽奖积分未入账）", len(entries))
	}
	for _, e := range entries {
		if e.Note != "连登抽奖" {
			t.Errorf("note=%q want 连登抽奖", e.Note)
		}
	}
}
