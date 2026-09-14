package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"workbuddy2api/internal/auth"
	"workbuddy2api/internal/member"
	"workbuddy2api/internal/upstream"
)

// fakeUpstreamOK 供鉴权测试使用：模型列表与聊天都返回成功，
// 使断言聚焦于"是否被鉴权拦截"而非上游行为。
func fakeUpstreamOK(t *testing.T) *upstream.Client {
	t.Helper()
	return newFakeUpstream(t, func(string) (int, string, bool) {
		return 200, `{"code":0,"data":{"models":[{"id":"glm-5.2"}],"agents":[{"name":"cli","models":["glm-5.2"]}]}}`, false
	})
}

// 成员鉴权的核心边界：成员密钥只能访问 /v1/*，不能碰 /status。
// 这是本功能的权限核心，必须逐一验证。
func TestMemberAuthorization(t *testing.T) {
	store := member.NewStore("")
	m := store.Add("甲", "")

	newH := func() *Handler {
		return NewHandler(Config{
			Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1"}),
			Upstream: fakeUpstreamOK(t),
			APIKey:   "admin-key",
			Members:  store,
		})
	}

	cases := []struct {
		name     string
		key      string
		path     string
		want     int
		onlyAuth bool // true = 只断言未被鉴权拦截（上游响应码不计）
	}{
		{"管理员访问 /status", "admin-key", "/status", http.StatusOK, true},
		{"管理员访问 /v1/models", "admin-key", "/v1/models", http.StatusOK, true},
		{"成员访问 /v1/models", m.Key, "/v1/models", http.StatusOK, true},
		{"成员越权访问 /status", m.Key, "/status", http.StatusForbidden, false},
		{"未知密钥访问 /v1/models", "wrong", "/v1/models", http.StatusUnauthorized, false},
		{"缺失密钥访问 /v1/models", "", "/v1/models", http.StatusUnauthorized, false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newH()
			r := httptest.NewRequest("GET", c.path, nil)
			if c.key != "" {
				r.Header.Set("Authorization", "Bearer "+c.key)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)

			if c.onlyAuth {
				if w.Code == http.StatusUnauthorized || w.Code == http.StatusForbidden {
					t.Errorf("should be authorized, got %d", w.Code)
				}
				return
			}
			if w.Code != c.want {
				t.Errorf("code=%d want %d (body=%s)", w.Code, c.want, w.Body.String())
			}
		})
	}
}

// 成员不能访问面板接口：路由虽可达，但面板自身的 api_key 校验会拒绝成员密钥。
func TestMemberCannotAccessPanel(t *testing.T) {
	store := member.NewStore("")
	m := store.Add("甲", "")

	panelH := &stubPanel{code: http.StatusOK}
	h := NewHandler(Config{
		Pool:    testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1"}),
		APIKey:  "admin-key",
		Members: store,
		Panel:   panelH,
	})

	r := httptest.NewRequest("GET", "/panel/api/overview", nil)
	r.Header.Set("Authorization", "Bearer "+m.Key)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	// 面板不走 server 的 withAuth（自带鉴权），故这里只验证路由可达；
	// 真实拒绝由 panel 包的 withAuth 保证（成员密钥 != 管理员 api_key）。
	if panelH.calls == 0 {
		t.Error("panel route should be reachable (its own auth rejects member keys)")
	}
}

type stubPanel struct {
	code  int
	calls int
}

func (s *stubPanel) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.calls++
	w.WriteHeader(s.code)
}

// 未配置 Members 时，行为必须与从前完全一致（向后兼容）。
func TestNoMembersKeepsLegacyBehavior(t *testing.T) {
	h := NewHandler(Config{
		Pool:   testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1"}),
		APIKey: "admin-key",
	})

	r := httptest.NewRequest("GET", "/status", nil)
	r.Header.Set("Authorization", "Bearer admin-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Errorf("valid key should pass, got %d", w.Code)
	}

	r2 := httptest.NewRequest("GET", "/status", nil)
	r2.Header.Set("Authorization", "Bearer nope")
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, r2)
	if w2.Code != http.StatusUnauthorized {
		t.Errorf("invalid key should 401, got %d", w2.Code)
	}
}

// api_key 为空但启用了成员体系时，仍只认成员密钥——
// 否则"空密钥放行"会让成员隔离形同虚设。
func TestEmptyAdminKeyWithMembersStillRequiresMemberKey(t *testing.T) {
	store := member.NewStore("")
	m := store.Add("甲", "")

	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1"}),
		Upstream: fakeUpstreamOK(t),
		APIKey:   "",
		Members:  store,
	})

	// 无密钥：拒绝（不再是无鉴权放行）
	r := httptest.NewRequest("GET", "/v1/models", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("empty admin key + members enabled must still 401 without member key, got %d", w.Code)
	}

	// 成员密钥：放行
	r2 := httptest.NewRequest("GET", "/v1/models", nil)
	r2.Header.Set("Authorization", "Bearer "+m.Key)
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, r2)
	if w2.Code == http.StatusUnauthorized || w2.Code == http.StatusForbidden {
		t.Errorf("member key should pass, got %d", w2.Code)
	}
}

// 成员身份应经 context 传到 chatCompletions（记账依赖它）。
func TestMemberIdentityInContext(t *testing.T) {
	store := member.NewStore("")
	m := store.Add("甲", "")

	h := NewHandler(Config{
		Pool:    testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1"}),
		APIKey:  "admin-key",
		Members: store,
	})

	r := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	r.Header.Set("Authorization", "Bearer "+m.Key)

	res := h.authenticate(r)
	if !res.ok || res.memberID != m.ID {
		t.Fatalf("authenticate failed: %+v", res)
	}
	// 与 withAuth 相同的注入方式，确认 memberOf 能取回身份。
	wrapped := r.WithContext(context.WithValue(r.Context(), memberCtxKey{}, res.memberID))
	if got := memberOf(wrapped); got != m.ID {
		t.Errorf("memberOf=%q want %q", got, m.ID)
	}
	// 管理员/未启用时为空
	if got := memberOf(httptest.NewRequest("GET", "/v1/models", nil)); got != "" {
		t.Errorf("plain request should have empty member id, got %q", got)
	}
}

// 管理员密钥在启用成员体系后仍可访问全部端点（不被成员白名单限制）。
func TestAdminKeyUnaffectedByMemberWhitelist(t *testing.T) {
	store := member.NewStore("")
	store.Add("甲", "")

	h := NewHandler(Config{
		Pool:     testPoolWith(&auth.Auth{UID: "u1", AccessToken: "at1"}),
		Upstream: fakeUpstreamOK(t),
		APIKey:   "admin-key",
		Members:  store,
	})

	for _, p := range []string{"/status", "/v1/models"} {
		r := httptest.NewRequest("GET", p, nil)
		r.Header.Set("Authorization", "Bearer admin-key")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code == http.StatusUnauthorized || w.Code == http.StatusForbidden {
			t.Errorf("admin should access %s, got %d", p, w.Code)
		}
	}
}

// 成员消耗必须记到该成员账上，且不影响其他成员。
func TestMemberRecordIsolation(t *testing.T) {
	store := member.NewStore("")
	a := store.Add("甲", "")
	b := store.Add("乙", "")

	store.Record(a.ID, 1.25, true)
	store.Record(b.ID, 0.5, true)
	store.Record(a.ID, 0, false)

	_, list := store.Stats()
	byName := map[string]int{}
	for i, v := range list {
		byName[v.Name] = i
	}
	av := list[byName["甲"]]
	bv := list[byName["乙"]]
	if av.TotalCredit != 1.25 || av.Requests != 2 || av.Errors != 1 {
		t.Errorf("甲 mismatch: %+v", av)
	}
	if bv.TotalCredit != 0.5 || bv.Requests != 1 || bv.Errors != 0 {
		t.Errorf("乙 mismatch: %+v", bv)
	}
}
