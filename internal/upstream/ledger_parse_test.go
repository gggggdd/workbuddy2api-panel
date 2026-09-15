package upstream

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"workbuddy2api/internal/auth"
)

// ---- 签到奖励解析 ----
//
// 回归背景：DailyCheckinCredit 原先只认 data.Response.Data 一层，字段缺失或
// 信封层级不同即静默返回 0；调用方（scheduler/panel/login）按 credit>0 才入账，
// 0 会被 ledger.Append 的零变动过滤丢弃——签到积分长期不入账查不到原因。

// TestParseCheckinCreditNested /billing/meter 族实测形状：data.Response.Data.credit。
func TestParseCheckinCreditNested(t *testing.T) {
	raw := json.RawMessage(`{"Response":{"Data":{"credit":9,"total_credit":500}}}`)
	credit, balance := parseCheckinCredit(raw)
	if credit != 9 || balance != 500 {
		t.Errorf("credit=%v balance=%v want 9/500", credit, balance)
	}
}

// TestParseCheckinCreditFlat 平铺形状（信封层级不同的上游版本）同样要能取到。
func TestParseCheckinCreditFlat(t *testing.T) {
	raw := json.RawMessage(`{"credit":12,"balance":800}`)
	credit, balance := parseCheckinCredit(raw)
	if credit != 12 || balance != 800 {
		t.Errorf("credit=%v balance=%v want 12/800", credit, balance)
	}
}

// TestParseCheckinCreditStringNumber 上游字段可能是字符串数字。
func TestParseCheckinCreditStringNumber(t *testing.T) {
	raw := json.RawMessage(`{"Response":{"Data":{"credit":"7","total_credit":"321"}}}`)
	credit, balance := parseCheckinCredit(raw)
	if credit != 7 || balance != 321 {
		t.Errorf("credit=%v balance=%v want 7/321", credit, balance)
	}
}

// TestParseCheckinCreditMissing 字段全缺 → (0,0)，调用方据此不入账，不臆造金额。
func TestParseCheckinCreditMissing(t *testing.T) {
	for _, raw := range []string{`{}`, `{"Response":{"Data":{}}}`, ``, `null`} {
		credit, balance := parseCheckinCredit(json.RawMessage(raw))
		if credit != 0 || balance != 0 {
			t.Errorf("raw=%q credit=%v balance=%v want 0/0", raw, credit, balance)
		}
	}
}

// TestParseCheckinCreditRewardFallback reward_credit 作为 credit 的别名。
func TestParseCheckinCreditRewardFallback(t *testing.T) {
	raw := json.RawMessage(`{"Response":{"Data":{"reward_credit":15}}}`)
	if credit, _ := parseCheckinCredit(raw); credit != 15 {
		t.Errorf("credit=%v want 15 (reward_credit fallback)", credit)
	}
}

// ---- 连登抽奖载荷解析 ----
//
// 回归背景：LotteryDraw 透传原始载荷、不解析，抽奖积分只进日志不入账
//（实测 72h 内 33 笔 / 1038c 未记录）。LotteryCredit 做宽松候选键匹配。

// TestLotteryCreditTopLevel 顶层 credit 字段。
func TestLotteryCreditTopLevel(t *testing.T) {
	code, credit := LotteryCredit(json.RawMessage(`{"prize_code":"growth_credit_66","credit":66}`))
	if credit != 66 || code != "growth_credit_66" {
		t.Errorf("code=%q credit=%v want growth_credit_66/66", code, credit)
	}
}

// TestLotteryCreditNestedPrize 奖品嵌在 prize 子对象里。
func TestLotteryCreditNestedPrize(t *testing.T) {
	raw := json.RawMessage(`{"prize":{"prize_code":"p6","credit_amount":6}}`)
	code, credit := LotteryCredit(raw)
	if credit != 6 || code != "p6" {
		t.Errorf("code=%q credit=%v want p6/6", code, credit)
	}
}

// TestLotteryCreditStringNumber 字符串数字。
func TestLotteryCreditStringNumber(t *testing.T) {
	if _, credit := LotteryCredit(json.RawMessage(`{"reward_credit":"9"}`)); credit != 9 {
		t.Errorf("credit=%v want 9", credit)
	}
}

// TestLotteryCreditVoucherNoCredit 实物券无积分字段 → credit=0（调用方不入账）。
func TestLotteryCreditVoucherNoCredit(t *testing.T) {
	_, credit := LotteryCredit(json.RawMessage(`{"prize_code":"school_voucher_luckin"}`))
	if credit != 0 {
		t.Errorf("credit=%v want 0 for voucher", credit)
	}
}

// TestLotteryCreditBadPayload 非法载荷不 panic、返回零值。
func TestLotteryCreditBadPayload(t *testing.T) {
	for _, raw := range []string{``, `null`, `[1,2]`, `not-json`} {
		code, credit := LotteryCredit(json.RawMessage(raw))
		if code != "" || credit != 0 {
			t.Errorf("raw=%q code=%q credit=%v want empty/0", raw, code, credit)
		}
	}
}

// ---- 开学季任务 / 抽奖字段 ----

// TestSchoolTasksParsesRewardCredit /tasks 返回 reward_credit，入账金额以它为准
//（领奖响应只回 chance_granted，不回积分）。字段名取自 2026-09-15 实测响应。
func TestSchoolTasksParsesRewardCredit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/portal/activity/school/tasks" {
			t.Errorf("path=%s", r.URL.Path)
		}
		w.Write([]byte(`{"code":0,"message":"ok","data":{"in_period":true,"tasks":[` +
			`{"task_code":"share_invite","status":"completed","progress":1,"target_count":1,` +
			`"reward_credit":100,"reward_chance":1,"task_type":"recurring"},` +
			`{"task_code":"chat_3_times","status":"pending","progress":0,"target_count":3,` +
			`"reward_credit":50,"reward_chance":1}]}}`))
	}))
	defer srv.Close()

	c := &Client{HTTP: srv.Client(), BillingBaseCN: srv.URL}
	tasks, inPeriod, err := c.SchoolTasks(&auth.Auth{AccessToken: "at", UID: "u1"})
	if err != nil {
		t.Fatalf("SchoolTasks: %v", err)
	}
	if !inPeriod || len(tasks) != 2 {
		t.Fatalf("inPeriod=%v tasks=%d want true/2", inPeriod, len(tasks))
	}
	if tasks[0].RewardCredit != 100 || tasks[1].RewardCredit != 50 {
		t.Errorf("reward credits=%d,%d want 100,50", tasks[0].RewardCredit, tasks[1].RewardCredit)
	}
}

// TestSchoolDrawReturnsCredit 转盘抽奖返回 (prize_code, credit_amount)，
// 积分不再被格式化成字符串后丢弃（原先只返回 "%s +%dc"）。
func TestSchoolDrawReturnsCredit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s want POST", r.Method)
		}
		w.Write([]byte(`{"code":0,"data":{"prize_code":"school_credit_66","credit_amount":66,"chance_balance":0}}`))
	}))
	defer srv.Close()

	c := &Client{HTTP: srv.Client(), BillingBaseCN: srv.URL}
	code, credit, err := c.SchoolDraw(&auth.Auth{AccessToken: "at", UID: "u1"})
	if err != nil {
		t.Fatalf("SchoolDraw: %v", err)
	}
	if code != "school_credit_66" || credit != 66 {
		t.Errorf("code=%q credit=%d want school_credit_66/66", code, credit)
	}
}

// TestSchoolDrawVoucherZeroCredit 实物券 credit_amount=0，不产生入账。
func TestSchoolDrawVoucherZeroCredit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"code":0,"data":{"prize_code":"school_voucher_kfc_ok","credit_amount":0}}`))
	}))
	defer srv.Close()

	c := &Client{HTTP: srv.Client(), BillingBaseCN: srv.URL}
	code, credit, err := c.SchoolDraw(&auth.Auth{AccessToken: "at", UID: "u1"})
	if err != nil {
		t.Fatalf("SchoolDraw: %v", err)
	}
	if code != "school_voucher_kfc_ok" || credit != 0 {
		t.Errorf("code=%q credit=%d want voucher/0", code, credit)
	}
}
