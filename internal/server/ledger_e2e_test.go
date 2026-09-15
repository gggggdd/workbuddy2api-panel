package server

import (
	"net/http/httptest"
	"strings"
	"testing"

	"workbuddy2api/internal/auth"
	"workbuddy2api/internal/ledger"
	"workbuddy2api/internal/member"
)

// sseCreditOK 上游流式响应：末帧带 usage.credit —— 成员/账本记账的数据来源。
// 既有 sseOK 不含 credit 字段，故此处单独构造。
const sseCreditOK = "data: {\"id\":\"chatcmpl-c\",\"object\":\"chat.completion.chunk\",\"created\":1753600000,\"model\":\"glm-5.2\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"你好\"}}]}\n\n" +
	"data: {\"id\":\"chatcmpl-c\",\"object\":\"chat.completion.chunk\",\"created\":1753600000,\"model\":\"glm-5.2\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2,\"credit\":1.25}}\n\n" +
	"data: [DONE]\n\n"

const chatReqBody = `{"model":"glm-5.2","messages":[{"role":"user","content":"hi"}]}`

// newLedgerHandler 组装带成员体系 + 账本的 handler（fake upstream 返回带 credit 的流）。
func newLedgerHandler(t *testing.T) (*Handler, *ledger.Store, *member.Store) {
	t.Helper()
	up := newFakeUpstream(t, func(string) (int, string, bool) { return 200, sseCreditOK, true })
	lgr := ledger.New("", 100)
	members := member.NewStore("")
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream: up,
		APIKey:   "admin-key",
		Members:  members,
		Ledger:   lgr,
	})
	return h, lgr, members
}

// TestChatLedgerRecordsNonStreamUsage 非流式：成功后账本记一笔 chat 消耗（负数），
// 并按 (账号, 模型, 成员) 归因。
//
// 回归保护：2026-09-15 该记账代码曾被整文件覆盖删除，流水与成员用量静默停更
// 数小时而无人察觉（编译通过、请求正常）。此测试走完整 HTTP 路径，任何再次
// 删除记账逻辑都会立即失败。
func TestChatLedgerRecordsNonStreamUsage(t *testing.T) {
	h, lgr, members := newLedgerHandler(t)
	m := members.Add("甲", "")

	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(chatReqBody))
	req.Header.Set("Authorization", "Bearer "+m.Key)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}

	entries := lgr.List(ledger.Query{Kind: ledger.KindChat})
	if len(entries) != 1 {
		t.Fatalf("ledger chat entries=%d want 1 (%+v)", len(entries), entries)
	}
	e := entries[0]
	if e.UID != "u1" || e.Model != "glm-5.2" || e.Member != m.ID {
		t.Errorf("entry attribution wrong: %+v", e)
	}
	if e.Delta != -1.25 {
		t.Errorf("delta=%v want -1.25", e.Delta)
	}

	_, list := members.Stats()
	if len(list) != 1 || list[0].Requests != 1 {
		t.Fatalf("member stats=%+v want requests=1", list)
	}
	if list[0].TotalCredit != 1.25 {
		t.Errorf("member credit=%v want 1.25", list[0].TotalCredit)
	}
}

// TestChatLedgerRecordsStreamUsage 流式：末帧 usage.credit 同样入账。
func TestChatLedgerRecordsStreamUsage(t *testing.T) {
	h, lgr, members := newLedgerHandler(t)
	m := members.Add("乙", "")

	req := httptest.NewRequest("POST", "/v1/chat/completions",
		strings.NewReader(`{"model":"glm-5.2","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer "+m.Key)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}

	entries := lgr.List(ledger.Query{Kind: ledger.KindChat})
	if len(entries) != 1 {
		t.Fatalf("ledger chat entries=%d want 1", len(entries))
	}
	if entries[0].Delta != -1.25 || entries[0].Member != m.ID {
		t.Errorf("entry=%+v want delta=-1.25 member=%s", entries[0], m.ID)
	}
}

// TestChatLedgerAdminKeyNoMember 管理员密钥：账本仍记（member 为空），成员计数不受影响。
func TestChatLedgerAdminKeyNoMember(t *testing.T) {
	h, lgr, members := newLedgerHandler(t)
	members.Add("丙", "") // 存在但不使用其密钥

	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(chatReqBody))
	req.Header.Set("Authorization", "Bearer admin-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("code=%d", rec.Code)
	}

	entries := lgr.List(ledger.Query{Kind: ledger.KindChat})
	if len(entries) != 1 {
		t.Fatalf("admin request should still be recorded, got %d", len(entries))
	}
	if entries[0].Member != "" {
		t.Errorf("admin entry member=%q want empty", entries[0].Member)
	}
	_, list := members.Stats()
	if list[0].Requests != 0 {
		t.Errorf("member must not be charged for admin traffic, got %d", list[0].Requests)
	}
}

// TestChatLedgerSkipsFailedRequest 失败请求不入账（不产生负数流水）。
func TestChatLedgerSkipsFailedRequest(t *testing.T) {
	up := newFakeUpstream(t, func(string) (int, string, bool) {
		return 500, `{"code":500,"msg":"boom"}`, false
	})
	lgr := ledger.New("", 100)
	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999}),
		Upstream: up,
		Ledger:   lgr,
	})
	req := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(chatReqBody))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code == 200 {
		t.Fatalf("expected failure, got 200")
	}
	if n := len(lgr.List(ledger.Query{})); n != 0 {
		t.Errorf("failed request must not be charged, got %d entries", n)
	}
}
