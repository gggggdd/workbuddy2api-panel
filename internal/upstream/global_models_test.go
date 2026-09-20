package upstream

import (
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"workbuddy2api/internal/auth"
)

// globalModelsSrv 返回一段 global 模型目录探测服务：calls 逐条记录请求路径，
// authz 记录最后一次鉴权头，respond 按路径决定响应。
func globalModelsSrv(t *testing.T, calls *[]string, authz *string, respond func(path string) (int, string)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*calls = append(*calls, r.URL.Path)
		if authz != nil {
			*authz = r.Header.Get("Authorization")
		}
		status, body := respond(r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
}

// enterpriseModelsResp 构造企业端点 /v2|/console/enterprises/personal/models 的响应。
func enterpriseModelsResp(ids ...string) string {
	var sb strings.Builder
	sb.WriteString(`{"code":0,"data":{"models":[`)
	for i, id := range ids {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`{"id":"` + id + `","name":"` + id + `"}`)
	}
	sb.WriteString(`]}}`)
	return sb.String()
}

// v3ConfigResp 构造 /v3/config 主路响应（v3-config-merge 后的动态目录主来源）。
func v3ConfigResp(ids ...string) string {
	var sb strings.Builder
	sb.WriteString(`{"code":0,"data":{"models":[`)
	for i, id := range ids {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`{"id":"` + id + `","name":"` + id + `"}`)
	}
	sb.WriteString(`]}}`)
	return sb.String()
}

func globalModelsClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	return &Client{
		HTTP:           &http.Client{},
		ChatBaseGlobal: strings.TrimSuffix(srv.URL, "/"),
		GlobalEnabled:  true,
	}
}

// probeCallsExceptV3 过滤掉 /v3/config 主路，只看企业端点家族的请求序列。
func probeCallsExceptV3(calls []string) []string {
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		if c != "/v3/config" {
			out = append(out, c)
		}
	}
	return out
}

// TestFetchGlobalModelsProbeMergesAndHeaders 探测命中：v3 主路 + 企业端点家族并发探测，
// 结果取并集去重；鉴权头带上 Bearer；disabled 条目不入并集。
//
// 契约变更（v3-config-merge）：/v3/config 与 /v2 家族**并发**两路，不再是"v2 家族首选
// 单路命中即止"——因此断言按"过滤 v3 后的家族序列"和"并集内容"两条来做。
func TestFetchGlobalModelsProbeMergesAndHeaders(t *testing.T) {
	auth.SetGlobalEnabled(true)
	t.Cleanup(func() { auth.SetGlobalEnabled(true) })

	var calls []string
	var gotAuthz string
	srv := globalModelsSrv(t, &calls, &gotAuthz, func(path string) (int, string) {
		if path == "/v3/config" {
			return 200, v3ConfigResp("v3-only-a", "shared-model")
		}
		return 200, enterpriseModelsResp("shared-model", "family-only-b")
	})
	defer srv.Close()

	got := globalModelsClient(t, srv).FetchGlobalModels(globalAcct())

	fam := probeCallsExceptV3(calls)
	if len(fam) != 1 || fam[0] != "/v2/enterprises/personal/models" {
		t.Fatalf("family calls=%v want [/v2/enterprises/personal/models]（家族首选命中即止）", fam)
	}
	if gotAuthz != "Bearer at" {
		t.Errorf("probe authz=%q want Bearer at", gotAuthz)
	}
	counts := map[string]int{}
	for _, id := range got {
		counts[id]++
	}
	// 并集去重：两路各自独有的都进来，同名只算一次。
	for _, id := range []string{"v3-only-a", "family-only-b", "shared-model"} {
		if counts[id] != 1 {
			t.Errorf("model %q appears %d times want 1 (union+dedupe)", id, counts[id])
		}
	}
}

// TestFetchGlobalModelsNoStaticFallback 两路全失败 → 返回空（纯动态，无静态回落）。
//
// 契约变更：旧实现失败会回落 GlobalModelNames 静态名单；现为纯动态——失败即空，
// 由调用方（handler）按 ID 名单输出裸条目，不编造未实测的模型。
func TestFetchGlobalModelsNoStaticFallback(t *testing.T) {
	auth.SetGlobalEnabled(true)
	t.Cleanup(func() { auth.SetGlobalEnabled(true) })

	var calls []string
	srv := globalModelsSrv(t, &calls, nil, func(path string) (int, string) {
		return 500, `{"code":500,"msg":"boom"}`
	})
	defer srv.Close()

	got := globalModelsClient(t, srv).FetchGlobalModels(globalAcct())

	fam := probeCallsExceptV3(calls)
	if len(fam) != 2 || fam[0] != "/v2/enterprises/personal/models" || fam[1] != "/console/enterprises/personal/models" {
		t.Fatalf("family calls=%v want [/v2/..., /console/...]（家族按序试完）", fam)
	}
	if len(got) != 0 {
		t.Errorf("pure-dynamic contract: failure must yield empty, got %v", got)
	}
}

// TestFetchGlobalModelsCache 成功后再调用命中 1h 缓存：零新上游请求，结果不变。
func TestFetchGlobalModelsCache(t *testing.T) {
	auth.SetGlobalEnabled(true)
	t.Cleanup(func() { auth.SetGlobalEnabled(true) })

	var calls []string
	srv := globalModelsSrv(t, &calls, nil, func(path string) (int, string) {
		if path == "/v3/config" {
			return 200, v3ConfigResp("gpt-5.4")
		}
		return 200, enterpriseModelsResp("probe-only-x")
	})
	defer srv.Close()

	c := globalModelsClient(t, srv)
	first := c.FetchGlobalModels(globalAcct())
	nAfterFirst := len(calls)
	second := c.FetchGlobalModels(globalAcct())

	if len(calls) != nAfterFirst {
		t.Errorf("cache: probe calls=%d -> %d want no new upstream calls (second hit cache)", nAfterFirst, len(calls))
	}
	if !reflect.DeepEqual(first, second) {
		t.Errorf("cached result differs from first")
	}
}

// TestFetchGlobalModelsNegativeCache 失败后进入负缓存：冷却期内再次调用零新请求，
// 且结果仍为空（不复活静态名单）。
func TestFetchGlobalModelsNegativeCache(t *testing.T) {
	auth.SetGlobalEnabled(true)
	t.Cleanup(func() { auth.SetGlobalEnabled(true) })

	var calls []string
	srv := globalModelsSrv(t, &calls, nil, func(path string) (int, string) {
		return 500, `{"code":500,"msg":"boom"}`
	})
	defer srv.Close()

	c := globalModelsClient(t, srv)
	first := c.FetchGlobalModels(globalAcct())
	nAfterFirst := len(calls)
	second := c.FetchGlobalModels(globalAcct())

	if len(calls) != nAfterFirst {
		t.Errorf("negative cache: calls=%d -> %d want no new calls within cooldown", nAfterFirst, len(calls))
	}
	if len(first) != 0 || len(second) != 0 {
		t.Errorf("negative-cache result must be empty (pure dynamic), got %v / %v", first, second)
	}
}

// TestFetchGlobalModelsParseNarrowTable 窄表形态（data 为字符串数组）也能解析并入并集。
func TestFetchGlobalModelsParseNarrowTable(t *testing.T) {
	auth.SetGlobalEnabled(true)
	t.Cleanup(func() { auth.SetGlobalEnabled(true) })

	// 窄表解析属于 parseGlobalModelNames（企业端点家族用）；/v3/config 主路走
	// 自己的严格对象解析（只认 data.models[]），故窄表桩必须挂企业端点路径。
	var calls []string
	srv := globalModelsSrv(t, &calls, nil, func(path string) (int, string) {
		if path == "/v3/config" {
			return 404, `{"code":404,"msg":"nope"}`
		}
		return 200, `{"code":0,"data":["gpt-5.4","narrow-only"]}`
	})
	defer srv.Close()

	got := globalModelsClient(t, srv).FetchGlobalModels(globalAcct())

	counts := map[string]int{}
	for _, id := range got {
		counts[id]++
	}
	if counts["gpt-5.4"] != 1 || counts["narrow-only"] != 1 {
		t.Errorf("narrow-table merge not as expected: %v", counts)
	}
}
