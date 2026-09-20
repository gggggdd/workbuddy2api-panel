package upstream

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"workbuddy2api/internal/auth"
)

// modelsBodyCLI 构造企业端点形态响应（agents[cli].models + models 元数据）。
func modelsBodyCLI(ids ...string) string {
	var sb strings.Builder
	sb.WriteString(`{"code":0,"data":{"agents":[{"name":"cli","models":[`)
	for i, id := range ids {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`"` + id + `"`)
	}
	sb.WriteString(`]}],"models":[`)
	for i, id := range ids {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`{"id":"` + id + `","name":"` + id + `","maxInputTokens":65536,"maxOutputTokens":8192,"disabled":false}`)
	}
	sb.WriteString(`]}}`)
	return sb.String()
}

// v3ModelsBody 构造 /v3/config 形态响应（v3-config-merge 的主路）。
func v3ModelsBody(ids ...string) string {
	var sb strings.Builder
	sb.WriteString(`{"code":0,"data":{"models":[`)
	for i, id := range ids {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(`{"id":"` + id + `","name":"` + id + `","maxInputTokens":131072,"maxOutputTokens":16384}`)
	}
	sb.WriteString(`]}}`)
	return sb.String()
}

// TestModelsPathCNUnchanged CN 企业端点仍走 /console（零回归），且 v3 主路并发参与并集。
func TestModelsPathCNUnchanged(t *testing.T) {
	auth.SetGlobalEnabled(true)
	t.Cleanup(func() { auth.SetGlobalEnabled(true) })

	var enterprisePath string
	c := testClient(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/v3/config":
			return jsonResp(200, v3ModelsBody("glm-5.2")), nil
		default:
			enterprisePath = r.URL.Path
			return jsonResp(200, modelsBodyCLI("glm-5.2")), nil
		}
	})
	cn := &auth.Auth{AccessToken: "at", UID: "cn1", Domain: "www.codebuddy.cn"}

	if _, err := c.FetchModels(cn); err != nil {
		t.Fatalf("cn fetch models: %v", err)
	}
	// CN 企业端点零回归：仍走 /console/enterprises/personal/models（现状逐字）。
	if enterprisePath != "/console/enterprises/personal/models" {
		t.Errorf("cn enterprise path=%q want /console/enterprises/personal/models", enterprisePath)
	}
}

// TestModelsPathGlobalUsesEnterpriseConsole global 的企业端点同样固定 /console
// （v3-config-merge 后企业补充路不再分叉 /v2 家族）。
func TestModelsPathGlobalUsesEnterpriseConsole(t *testing.T) {
	auth.SetGlobalEnabled(true)
	t.Cleanup(func() { auth.SetGlobalEnabled(true) })

	var hit []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = append(hit, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		if r.URL.Path == "/v3/config" {
			_, _ = w.Write([]byte(v3ModelsBody("gpt-5.4")))
			return
		}
		_, _ = w.Write([]byte(modelsBodyCLI("gpt-5.4")))
	}))
	defer srv.Close()

	c := &Client{
		HTTP:           http.DefaultClient,
		ChatBaseGlobal: strings.TrimSuffix(srv.URL, "/"),
		GlobalEnabled:  true,
	}
	infos, err := c.FetchModels(globalAcct())
	if err != nil {
		t.Fatalf("global fetch models: %v", err)
	}
	if len(infos) == 0 {
		t.Fatal("global fetch models returned empty")
	}
	var sawV3, sawConsole bool
	for _, p := range hit {
		if p == "/v3/config" {
			sawV3 = true
		}
		if p == "/console/enterprises/personal/models" {
			sawConsole = true
		}
	}
	if !sawV3 {
		t.Errorf("global probe paths=%v want /v3/config probed (v3 main path)", hit)
	}
	if !sawConsole {
		t.Errorf("global probe paths=%v want /console/enterprises/personal/models (enterprise supplement)", hit)
	}
}

// TestGlobalModelsProbeV3AndFamilyMerge v3 主路 + 企业端点家族并发探测，
// 家族内部仍按 /v2 → /console 顺序（v2 成功即止）。
func TestGlobalModelsProbeV3AndFamilyMerge(t *testing.T) {
	auth.SetGlobalEnabled(true)
	t.Cleanup(func() { auth.SetGlobalEnabled(true) })

	var calls []string
	srv := globalModelsSrv(t, &calls, nil, func(path string) (int, string) {
		switch path {
		case "/v3/config":
			return 200, v3ModelsBody("v3-only")
		case "/v2/enterprises/personal/models":
			return 200, enterpriseModelsResp("family-only")
		case "/console/enterprises/personal/models":
			return 500, `{"code":500,"msg":"boom"}`
		}
		return 404, `{"code":404,"msg":"nope"}`
	})
	defer srv.Close()

	got := globalModelsClient(t, srv).FetchGlobalModels(globalAcct())

	fam := probeCallsExceptV3(calls)
	if len(fam) != 1 || fam[0] != "/v2/enterprises/personal/models" {
		t.Fatalf("family calls=%v want [/v2/...] (v2-first within family)", fam)
	}
	counts := map[string]int{}
	for _, id := range got {
		counts[id]++
	}
	if counts["v3-only"] != 1 || counts["family-only"] != 1 {
		t.Fatalf("v3+family merge not as expected: %v", counts)
	}
}

// TestGlobalModelsProbeConsoleFallback 家族内 /v2 失败 → 回落 /console（顺序语义保留）。
func TestGlobalModelsProbeConsoleFallback(t *testing.T) {
	auth.SetGlobalEnabled(true)
	t.Cleanup(func() { auth.SetGlobalEnabled(true) })

	var calls []string
	srv := globalModelsSrv(t, &calls, nil, func(path string) (int, string) {
		switch path {
		case "/v3/config":
			return 404, `{"code":404,"msg":"nope"}`
		case "/v2/enterprises/personal/models":
			return 500, `{"code":500,"msg":"boom"}`
		case "/console/enterprises/personal/models":
			return 200, enterpriseModelsResp("legacy-only")
		}
		return 404, `{"code":404,"msg":"nope"}`
	})
	defer srv.Close()

	got := globalModelsClient(t, srv).FetchGlobalModels(globalAcct())

	fam := probeCallsExceptV3(calls)
	if len(fam) != 2 ||
		fam[0] != "/v2/enterprises/personal/models" ||
		fam[1] != "/console/enterprises/personal/models" {
		t.Fatalf("family calls=%v want [/v2/..., /console/...]", fam)
	}
	counts := map[string]int{}
	for _, id := range got {
		counts[id]++
	}
	if counts["legacy-only"] != 1 {
		t.Errorf("console fallback name not merged: %v", counts)
	}
}

// TestGlobalModelsProbePathsOrderStable 家族候选序列固定：/v2 在前、/console 兜底。
func TestGlobalModelsProbePathsOrderStable(t *testing.T) {
	if len(globalModelsProbePaths) != 2 {
		t.Fatalf("globalModelsProbePaths=%v want 2 entries (v2 + console fallback)", globalModelsProbePaths)
	}
	if globalModelsProbePaths[0] != "/v2/enterprises/personal/models" {
		t.Errorf("probePaths[0]=%q want /v2/enterprises/personal/models (newest first)", globalModelsProbePaths[0])
	}
	if globalModelsProbePaths[1] != "/console/enterprises/personal/models" {
		t.Errorf("probePaths[1]=%q want /console/enterprises/personal/models (fallback)", globalModelsProbePaths[1])
	}
}
