// hub-gateway：WorkBuddy / Trae / Qoder 三家反代的统一聚合网关。
//
// 对外暴露单一端口与单一 API key：
//   - GET  /v1/models             聚合三家模型目录（id 加前缀路由：无前缀=workbuddy，trae/xxx，qoder/xxx）
//   - POST /v1/chat/completions   按模型前缀转发到对应后端（流式透传，SSE 不缓冲）
//   - GET  /healthz               网关自身健康 + 三后端可达性快照
//
// 转发语义：网关只校验自己的 key，通过后**替换**为对应后端的 key 转发——三家
// 后端各自独立鉴权，互不知晓。SSE 场景 flush_interval 关闭逐字透传，超时对齐
// Caddy 现有配置（response header 600s）。
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// modelsClient 供 fetchModels 复用：每次新建会丢弃连接池，healthz 高频调用时
// 产生大量 TIME_WAIT。
var modelsClient = &http.Client{Timeout: 15 * time.Second}

type backend struct {
	name   string          // 诊断用
	prefix string          // 模型 id 前缀（"" = workbuddy 默认）
	target *url.URL        // 上游 base
	key    string          // 上游 API key（替换 Authorization）
	keyHdr string          // key 放哪个头（Qoder bridge 用 x-api-key，其余 Bearer）
	proxy  *httputil.ReverseProxy

	mu     sync.RWMutex
	models []modelEntry // 启动时探测的模型（无前缀），聚合时加前缀
}

// modelsSnapshot 读副本：遍历期间 backend 可能正被 healthz 重探写入，
// 无锁会导致 data race。返回浅拷贝（[]modelEntry 元素不可变，安全）。
func (b *backend) modelsSnapshot() []modelEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return append([]modelEntry(nil), b.models...)
}

// setModels 写入探测结果（healthz 复查 / 惰性重探共用）。
func (b *backend) setModels(ms []modelEntry) {
	b.mu.Lock()
	b.models = ms
	b.mu.Unlock()
}

// modelsCount 只取长度（供日志/健康检查，不需要整份拷贝）。
func (b *backend) modelsCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.models)
}

// modelEntry 是聚合目录里的一条：除 id 外保留后端给的积分倍率（rate，形如
// "x0.79"），供 /v1/models 透出——三家口径不同（workbuddy 直接给 credits，
// qoder 给数字 price_factor，trae 上游不提供），统一在这里收敛成字符串。
type modelEntry struct {
	ID            string
	Name          string   // 官方全名（可空：trae 上游不给）
	Rate          string   // 积分倍率（可空：trae 上游不给）
	Efforts       []string // 推理档位（可空：trae 上游不给）
	DefaultEffort string
	CanDisable    bool // 能否关闭思考
	// 上游原始字段全量保留（context_length / max_output_tokens / supports_* /
	// description / tags 等），透传给 /v1/models 让客户端能拿到与上游一致的元数据。
	Raw map[string]any
}

var (
	listen    = envOr("HUB_LISTEN", ":7860")
	hubKey    = envOr("HUB_API_KEY", "")
	wbTarget  = envOr("HUB_WB_TARGET", "http://127.0.0.1:7863")
	wbKey     = envOr("HUB_WB_KEY", "")
	traeTarget = envOr("HUB_TRAE_TARGET", "http://127.0.0.1:7864")
	traeKey   = envOr("HUB_TRAE_KEY", "")
	qoderTarget = envOr("HUB_QODER_TARGET", "http://127.0.0.1:8963")
	qoderKey  = envOr("HUB_QODER_KEY", "qccg")
	qoderConsolePassword = envOr("HUB_QODER_CONSOLE_PASSWORD", "")
	qoderConsoleTarget  = envOr("HUB_QODER_CONSOLE_TARGET", "http://127.0.0.1:3588")
	errNoSession = errorString("qoder console session not obtained")
)

// hubKeys 是网关接受的全部 API key：HUB_API_KEY 必填，HUB_EXTRA_KEYS 可追加
// （逗号分隔）。加 workbuddy 面板 key 进来，是为了让 /v1/* 切到网关后既有客户端
// 不用换 key——同一把 key 即可拿到三家聚合模型。
var hubKeys = map[string]bool{}

type errorString string

func (e errorString) Error() string { return string(e) }

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func mustTarget(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("bad target %q: %v", raw, err)
	}
	return u
}

func newBackend(name, prefix, rawTarget, key, keyHdr string) *backend {
	b := &backend{name: name, prefix: prefix, target: mustTarget(rawTarget), key: key, keyHdr: keyHdr}
	p := httputil.NewSingleHostReverseProxy(b.target)
	orig := p.Director
	p.Director = func(r *http.Request) {
		orig(r)
		r.Host = b.target.Host
		// 鉴权替换：剥掉调用方的 hub key，换成后端自己的 key。
		r.Header.Del("Authorization")
		if keyHdr == "x-api-key" {
			r.Header.Set("x-api-key", key)
		} else if key != "" {
			r.Header.Set("Authorization", "Bearer "+key)
		}
	}
	// SSE 透传：ReverseProxy 默认即流式（无缓冲），FlushInterval=-1 强制即时 flush。
	p.FlushInterval = -1
	p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("[hub] %s proxy error: %v", name, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		io.WriteString(w, `{"error":{"message":"backend `+name+` unavailable","type":"hub_bad_gateway","code":"hub_502"}}`)
	}
	b.proxy = p
	return b
}

// fetchModels 探测后端模型目录（启动时一次 + /healthz 复查）。失败不致命：返回
// 空（该后端在 /v1/models 里缺席，转发仍可用）。
func (b *backend) fetchModels() []modelEntry {
	req, _ := http.NewRequest(http.MethodGet, b.target.String()+"/v1/models", nil)
	if b.keyHdr == "x-api-key" {
		req.Header.Set("x-api-key", b.key)
	} else if b.key != "" {
		req.Header.Set("Authorization", "Bearer "+b.key)
	}
	resp, err := modelsClient.Do(req)
	if err != nil {
		log.Printf("[hub] %s models probe failed: %v", b.name, err)
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("[hub] %s models probe status %d", b.name, resp.StatusCode)
		return nil
	}
	var out struct {
		Data []map[string]any `json:"data"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out) != nil {
		return nil
	}
	entries := make([]modelEntry, 0, len(out.Data))
	for _, raw := range out.Data {
		id, _ := raw["id"].(string)
		if id == "" {
			continue
		}
		e := modelEntry{ID: id, Raw: raw}
		// 官方全名：qoder 用 display_name（qmodel_38max → Qwen3.8-Max），
		// workbuddy 用 name（cn:hy3 → Hy3），trae 两者都不给。
		if v, _ := raw["display_name"].(string); v != "" {
			e.Name = v
		} else if v, _ := raw["name"].(string); v != "" {
			e.Name = v
		}
		// 倍率：workbuddy 给字符串 "x0.79"；qoder 给数字 price_factor。
		if v, _ := raw["credits"].(string); v != "" {
			e.Rate = v
		} else if v, ok := raw["price_factor"]; ok && v != nil {
			if f, ok := v.(float64); ok {
				e.Rate = fmt.Sprintf("x%g", f)
			}
		}
		// 档位：workbuddy 用 reasoning_* 前缀，qoder bridge 用 efforts。
		if v, ok := raw["reasoning_supported_efforts"].([]any); ok {
			e.Efforts = toStrings(v)
		} else if v, ok := raw["efforts"].([]any); ok {
			e.Efforts = toStrings(v)
		}
		if v, _ := raw["reasoning_default_effort"].(string); v != "" {
			e.DefaultEffort = v
		} else if v, _ := raw["default_effort"].(string); v != "" {
			e.DefaultEffort = v
		}
		if v, _ := raw["can_disable_thinking"].(bool); v {
			e.CanDisable = true
		}
		entries = append(entries, e)
	}
	return entries
}

// pickBackend 按模型名前缀选后端：trae/、qoder/ 前缀精确匹配；其余走 workbuddy。
// 命中前缀后剥掉前缀（后端只认识自己的裸模型名）。
func pickBackend(model string) (*backend, string) {
	for _, b := range backends {
		if b.prefix != "" && strings.HasPrefix(model, b.prefix+"/") {
			return b, strings.TrimPrefix(model, b.prefix+"/")
		}
	}
	return backends[0], model // backends[0] = workbuddy（无前缀）
}

var backends []*backend

func main() {
	if hubKey == "" {
		log.Fatal("HUB_API_KEY is required")
	}
	hubKeys[hubKey] = true
	for _, k := range strings.Split(envOr("HUB_EXTRA_KEYS", ""), ",") {
		if k = strings.TrimSpace(k); k != "" {
			hubKeys[k] = true
		}
	}
	backends = []*backend{
		newBackend("workbuddy", "", wbTarget, wbKey, "bearer"),
		newBackend("trae", "trae", traeTarget, traeKey, "bearer"),
		newBackend("qoder", "qoder", qoderTarget, qoderKey, "x-api-key"),
	}
	for _, b := range backends {
		b.setModels(b.fetchModels())
		log.Printf("[hub] %s: %d models", b.name, b.modelsCount())
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("GET /v1/models", withAuth(listModels))
	mux.HandleFunc("POST /v1/chat/completions", withAuth(chat))
	mux.HandleFunc("POST /v1/messages", withAuth(chat))       // Anthropic 形态：仅 qoder 支持，透传
	mux.HandleFunc("POST /v1/responses", withAuth(chat))      // Responses 形态：仅 qoder 支持，透传
	// 管理面聚合（panel 消费）：跨厂商账号列表 / OAuth 登录代理 / 分厂商模型目录。
	mux.HandleFunc("GET /hub/api/accounts", withAuth(adminAccounts))
	mux.HandleFunc("POST /hub/api/oauth/start", withAuth(adminOAuthStart))
	mux.HandleFunc("POST /hub/api/oauth/wait", withAuth(adminOAuthWait))
	mux.HandleFunc("GET /hub/api/models", withAuth(adminModels))
	mux.HandleFunc("/hub/api/checkin", withAuth(adminCheckin))
	mux.HandleFunc("POST /hub/api/oauth/complete", withAuth(adminOAuthComplete)) // GET=状态 POST=手动触发
	// 其余路径（面板页 /panel/、面板管理接口 /panel/api/*）原样透给 workbuddy 本体：
	// 网关占住 7863 后面板不能凭空消失，且面板自己有独立鉴权，网关不再拦一道。
	panelProxy := httputil.NewSingleHostReverseProxy(backends[0].target)
	panelProxy.FlushInterval = -1
	// 与 backends 的 ErrorHandler 对齐：workbuddy 不可达时也返回 JSON，
	// 否则面板路径在故障时输出 Go 默认的 502 文本，破坏客户端 JSON 解析。
	panelProxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("[hub] panel proxy error: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		io.WriteString(w, `{"error":{"message":"workbuddy unavailable","type":"hub_bad_gateway","code":"hub_502"}}`)
	}
	mux.Handle("/", panelProxy)
	log.Printf("[hub] listening on %s", listen)
	log.Fatal(http.ListenAndServe(listen, mux))
}

// withAuth 校验 hub 自身 key（Bearer 形态）。
func withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authz := r.Header.Get("Authorization")
		if !strings.HasPrefix(authz, "Bearer ") || !hubKeys[strings.TrimPrefix(authz, "Bearer ")] {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			io.WriteString(w, `{"error":{"message":"invalid api key","type":"auth_error","code":"401"}}`)
			return
		}
		next(w, r)
	}
}

// listModels 聚合三家目录：workbuddy 裸名直出，trae/qoder 加前缀。
// model 字段带 prefix 的同时， owned_by 标后端名，客户端可据此区分。
//
// 输出策略：上游原始字段全量保留（context_length / max_output_tokens /
// supports_* / description / tags 等），外加统一字段名（name / rate /
// supported_efforts / default_effort / can_disable_thinking）——客户端可按需取用。
func listModels(w http.ResponseWriter, r *http.Request) {
	out := make([]map[string]any, 0, 64)
	now := time.Now().Unix()
	for _, b := range backends {
		ms := b.modelsSnapshot()
		if len(ms) == 0 {
			ms = b.fetchModels() // 惰性重探：后端恢复后目录自动补全
			b.setModels(ms)
		}
		for _, m := range ms {
			full := m.ID
			if b.prefix != "" {
				full = b.prefix + "/" + m.ID
			}
			// 以原始字段为底，再叠加统一字段与厂商标记。
			e := map[string]any{}
			for k, v := range m.Raw {
				e[k] = v
			}
			e["id"] = full
			e["object"] = "model"
			e["created"] = now
			e["owned_by"] = b.name
			if m.Name != "" {
				e["name"] = m.Name
			}
			if m.Rate != "" {
				e["rate"] = m.Rate
			}
			if len(m.Efforts) > 0 {
				e["supported_efforts"] = m.Efforts
			}
			if m.DefaultEffort != "" {
				e["default_effort"] = m.DefaultEffort
			}
			if m.CanDisable {
				e["can_disable_thinking"] = true
			}
			out = append(out, e)
		}
	}
	writeJSON(w, map[string]any{"object": "list", "data": out})
}

// chat 转发核心：读 body 取 model → 选后端 → 剥前缀 → 整包转发。
// body 大小钳 32M（workbuddy 上游限 32M），SSE 由 ReverseProxy 流式透传。
func chat(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close() // 替换 body 前先关闭原始的，避免连接资源滞留
	body, err := io.ReadAll(io.LimitReader(r.Body, 32<<20))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	var req struct {
		Model string `json:"model"`
	}
	if json.Unmarshal(body, &req) != nil || req.Model == "" {
		writeErr(w, http.StatusBadRequest, `body must be JSON with "model" field`)
		return
	}
	b, bare := pickBackend(req.Model)
	// 剥前缀：qoder/deepseek-v4 → deepseek-v4（后端只认裸名）。
	if bare != req.Model {
		var generic map[string]any
		if json.Unmarshal(body, &generic) == nil {
			generic["model"] = bare
			body, _ = json.Marshal(generic)
		}
	}
	log.Printf("[hub] %s <- model=%s (as %s)", b.name, req.Model, bare)
	r.Body = io.NopCloser(strings.NewReader(string(body)))
	r.ContentLength = int64(len(body))
	r.Header.Set("Content-Type", "application/json")
	b.proxy.ServeHTTP(w, r)
}

func healthz(w http.ResponseWriter, r *http.Request) {
	status := map[string]any{"ok": true}
	for _, b := range backends {
		ms := b.fetchModels()
		b.setModels(ms)
		status[b.name] = map[string]any{"reachable": len(ms) > 0 || probeOK(b), "models": len(ms)}
	}
	writeJSON(w, status)
}

// probeOK models 空也可能是后端正常但无账号——用 healthz 端点兜底判活。
func probeOK(b *backend) bool {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(b.target.String() + "/healthz")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// toStrings 把 []any 收拢成 []string（上游 JSON 数组里的元素都是 string）。
func toStrings(v []any) []string {
	out := make([]string, 0, len(v))
	for _, x := range v {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	io.WriteString(w, `{"error":{"message":"`+msg+`","type":"hub_error"}}`)
}
