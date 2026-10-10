// hub-gateway：WorkBuddy / Trae / Qoder 三家反代的统一聚合网关。
//
// 对外暴露单一端口与单一 API key：
//   - GET  /v1/models             聚合三家模型目录（id 加前缀路由：无前缀=workbuddy，trae/xxx，qoder/xxx）
//   - POST /v1/chat/completions   按模型前缀转发到对应后端（流式透传，SSE 不缓冲）
//   - GET  /healthz               网关自身健康 + 三后端可达性快照
//
// 转发语义：网关只校验自己的 key，通过后**替换**为对应后端的 key 转发——三家
// 后端各自独立鉴权，互不知晓。workbuddy 后端例外：调用方用的是本体签发的 key
// （主 key / 成员 key）时原样透传，本体才能把用量归因到成员。
// SSE 场景 flush_interval 关闭逐字透传，超时对齐 Caddy 现有配置
// （response header 600s）。
package main

import (
	"context"
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
	"sync/atomic"
	"time"
)

// modelsClient 供 fetchModels 复用：每次新建会丢弃连接池，healthz 高频调用时
// 产生大量 TIME_WAIT。
var modelsClient = &http.Client{Timeout: 15 * time.Second}

// /healthz 探测预算。旧实现每个请求都对三后端**串行** fetchModels（15s 超时）
// + 必要时 probeOK（5s），最坏单请求 45s+，16 并发实测 10s 超时；且探测失败时
// setModels(nil) 会把模型目录清空（连带 /v1/models 掉模型）。现在改成
// 「TTL 快照 + single-flight + 并发探测（单轮 ≤5s）」，形状与旧版严格一致。
const (
	healthzTTL      = 25 * time.Second // 快照新鲜期：窗口内直接命中缓存，不打上游
	healthzBudget   = 5 * time.Second  // 单次刷新总预算（三后端并发，硬上限）
	healthzModelCap = 4 * time.Second  // 单后端目录探测上限，留 1s 给 /healthz 兜底判活
)

// backendHealth 是 healthz 快照里的单条：形状与旧实现一致
// （{"models":N,"reachable":bool}），监控/面板按字段取值，不可改。
type backendHealth struct {
	Models    int  `json:"models"`
	Reachable bool `json:"reachable"`
}

// healthz 快照缓存：healthzAt 为零表示还没探过（冷启动），healthzFlight 非 nil
// 表示有刷新在进行，等待者 close 后读同一份结果。
var (
	healthzMu     sync.Mutex
	healthzAt     time.Time
	healthzSnap   map[string]backendHealth
	healthzFlight chan struct{}
)

type backend struct {
	name          string   // 诊断用
	prefix        string   // 模型 id 前缀（"" = workbuddy 默认）
	target        *url.URL // 上游 base
	key           string   // 上游 API key（替换 Authorization）
	keyHdr        string   // key 放哪个头（Qoder bridge 用 x-api-key，其余 Bearer）
	keepCallerKey bool     // 调用方 key 由该后端签发时原样透传（保留成员用量归因）
	proxy         *httputil.ReverseProxy

	mu     sync.RWMutex
	models []modelEntry // 启动时探测的模型（无前缀），聚合时加前缀
	// modelsAt 上次成功探测的时间；超过 modelsTTL 后由 /v1/models 触发后台刷新
	// （上游新增模型如 space-bunny 曾因不刷新而长期缺席，直到 hub 重启才出现）。
	modelsAt   time.Time
	refreshing atomic.Bool // 刷新 single-flight：并发请求只派一个探测
}

// modelsTTL 模型目录的新鲜期：超过后 /v1/models 会触发一次重新探测。
// 与面板侧模型缓存同哲学（上游目录变化频率远低于访问频率）；失败保留旧表。
const modelsTTL = 10 * time.Minute

// maybeRefreshModels 过期即重探（single-flight；失败保留旧表，下个窗口再试）。
func (b *backend) maybeRefreshModels() {
	b.mu.RLock()
	stale := len(b.models) == 0 || time.Since(b.modelsAt) > modelsTTL
	b.mu.RUnlock()
	if !stale || !b.refreshing.CompareAndSwap(false, true) {
		return
	}
	defer b.refreshing.Store(false)
	if ms := b.fetchModels(context.Background()); len(ms) > 0 {
		b.setModels(ms)
	}
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
	b.modelsAt = time.Now()
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
	listen               = envOr("HUB_LISTEN", ":7860")
	hubKey               = envOr("HUB_API_KEY", "")
	wbTarget             = envOr("HUB_WB_TARGET", "http://127.0.0.1:7863")
	wbKey                = envOr("HUB_WB_KEY", "")
	traeTarget           = envOr("HUB_TRAE_TARGET", "http://127.0.0.1:7864")
	traeKey              = envOr("HUB_TRAE_KEY", "")
	qoderTarget          = envOr("HUB_QODER_TARGET", "http://127.0.0.1:8963")
	qoderKey             = envOr("HUB_QODER_KEY", "qccg")
	qoderConsolePassword = envOr("HUB_QODER_CONSOLE_PASSWORD", "")
	qoderConsoleTarget   = envOr("HUB_QODER_CONSOLE_TARGET", "http://127.0.0.1:3588")
	errNoSession         = errorString("qoder console session not obtained")
)

// hubKeys 是网关接受的全部 API key：HUB_API_KEY 必填，HUB_EXTRA_KEYS 可追加
// （逗号分隔）。加 workbuddy 面板 key 进来，是为了让 /v1/* 切到网关后既有客户端
// 不用换 key——同一把 key 即可拿到三家聚合模型。
//
// wbOriginKeys 是 workbuddy 本体签发的 key：主 key + 面板成员 key。这些 key 对
// workbuddy 后端原样透传，本体才能把用量归因到成员。
//
// 两表均由 rebuildKeys 重建（env 口径 + HUB_KEYS_FILE 热加载口径），读写走
// keysMu：面板签发/轮换/删除成员写 members.json 后，网关 5s 内自动跟进——
// 新 key 即签即用，删除/轮换后旧 key 自动失效，不再依赖 sync-keys.sh 手动
// 同步 + 重启（曾导致新签成员 key 被 401、成员用量归因丢失）。
var (
	keysMu       sync.RWMutex
	hubKeys      = map[string]bool{}
	wbOriginKeys = map[string]bool{}
)

// keysFile 面板成员密钥文件（HUB_KEYS_FILE，默认 /srv/keys/members.json，
// 由 docker-compose 从 workbuddy2api-panel/data/members.json 只读挂载）。
var keysFile = envOr("HUB_KEYS_FILE", "/srv/keys/members.json")

// loadFileKeys 读 members.json（顶层数组）里的全部成员 key（解析失败/文件缺失
// 返回 nil，维持 env 口径不动；本体写文件是原子语义，读侧最多错过一轮，下轮自愈）。
func loadFileKeys() map[string]bool {
	b, err := os.ReadFile(keysFile)
	if err != nil {
		return nil
	}
	var members []struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(b, &members); err != nil {
		log.Printf("[hub] keys file %s parse: %v", keysFile, err)
		return nil
	}
	m := map[string]bool{}
	for _, x := range members {
		if k := strings.TrimSpace(x.Key); k != "" {
			m[k] = true
		}
	}
	return m
}

// rebuildKeys 从 env + 密钥文件整体重建两表后原子换入。
func rebuildKeys() {
	hub := map[string]bool{hubKey: true}
	origin := map[string]bool{}
	for _, k := range strings.Split(envOr("HUB_EXTRA_KEYS", ""), ",") {
		if k = strings.TrimSpace(k); k != "" {
			hub[k] = true
			origin[k] = true
		}
	}
	if fileKeys := loadFileKeys(); fileKeys != nil {
		for k := range fileKeys {
			hub[k] = true
			origin[k] = true
		}
	}
	keysMu.Lock()
	hubKeys, wbOriginKeys = hub, origin
	keysMu.Unlock()
}

// watchKeys 启动时装载一次，之后每 5s 看 mtime/size，变了才重建。
func watchKeys() {
	rebuildKeys()
	var lastM time.Time
	var lastS int64
	go func() {
		for range time.Tick(5 * time.Second) {
			st, err := os.Stat(keysFile)
			if err != nil || (st.ModTime() == lastM && st.Size() == lastS) {
				continue
			}
			lastM, lastS = st.ModTime(), st.Size()
			rebuildKeys()
		}
	}()
}

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

// bearerOf 取 Authorization 头里的 Bearer token（无则空串）。
func bearerOf(r *http.Request) string {
	authz := r.Header.Get("Authorization")
	if !strings.HasPrefix(authz, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(authz, "Bearer ")
}

func newBackend(name, prefix, rawTarget, key, keyHdr string, keepCallerKey bool) *backend {
	b := &backend{name: name, prefix: prefix, target: mustTarget(rawTarget), key: key, keyHdr: keyHdr, keepCallerKey: keepCallerKey}
	p := httputil.NewSingleHostReverseProxy(b.target)
	orig := p.Director
	p.Director = func(r *http.Request) {
		orig(r)
		r.Host = b.target.Host
		// 鉴权替换：剥掉调用方的 hub key，换成后端自己的 key。
		//
		// 例外：workbuddy 本体认得自己签发的 key（主 key 与成员 key，见 wbOriginKeys），
		// 原样透传——本体据此把用量归因到具体成员。若一律换成 HUB_WB_KEY，本体只看到
		// 主 key，成员用量会全部记不上（面板「成员管理」恒为 0）。
		caller := bearerOf(r)
		r.Header.Del("Authorization")
		keysMu.RLock()
		originKey := wbOriginKeys[caller]
		keysMu.RUnlock()
		if keyHdr == "x-api-key" {
			r.Header.Set("x-api-key", key)
		} else if b.keepCallerKey && originKey {
			r.Header.Set("Authorization", "Bearer "+caller)
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

// fetchModels 探测后端模型目录（启动时一轮 + /healthz 复查 + TTL 惰性重探）。
// ctx 用来兜住探测预算（healthz 单轮 ≤5s）；失败不致命：返回空（该后端在
// /v1/models 里缺席，转发仍可用）。
func (b *backend) fetchModels(ctx context.Context) []modelEntry {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.target.String()+"/v1/models", nil)
	if err != nil {
		return nil
	}
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
	watchKeys() // env 口径 + members.json 热加载（签发/轮换/删除成员自动生效）
	backends = []*backend{
		newBackend("workbuddy", "", wbTarget, wbKey, "bearer", true),
		newBackend("trae", "trae", traeTarget, traeKey, "bearer", false),
		newBackend("qoder", "qoder", qoderTarget, qoderKey, "x-api-key", false),
	}
	startupProbe() // 并发探一轮（≤healthzBudget）并装进 healthz 首帧缓存

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("GET /v1/models", withAuth(listModels))
	mux.HandleFunc("POST /v1/chat/completions", withAuth(chat))
	mux.HandleFunc("POST /v1/messages", withAuth(chat))  // Anthropic 形态：仅 qoder 支持，透传
	mux.HandleFunc("POST /v1/responses", withAuth(chat)) // Responses 形态：仅 qoder 支持，透传
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

// withAuth 校验 hub 自身 key（Bearer 形态）。key 表可能被 watchKeys 并发重建，
// 读快照走读锁。
func withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authz := r.Header.Get("Authorization")
		keysMu.RLock()
		ok := strings.HasPrefix(authz, "Bearer ") && hubKeys[strings.TrimPrefix(authz, "Bearer ")]
		keysMu.RUnlock()
		if !ok {
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
		b.maybeRefreshModels() // TTL 过期即重探（含启动后从未探到的惰性探活）
		ms := b.modelsSnapshot()
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

// healthz 运维/监控入口：读 TTL 快照（过期才刷新一次），响应形状与旧版严格一致
// （{"ok":true,"<name>":{"models":N,"reachable":bool}}），不加鉴权。
func healthz(w http.ResponseWriter, r *http.Request) {
	snap := healthSnapshot(r.Context())
	status := map[string]any{"ok": true}
	for _, b := range backends {
		status[b.name] = snap[b.name]
	}
	writeJSON(w, status)
}

// healthSnapshot 取健康快照：TTL 内直接命中；过期由先到者刷新（single-flight），
// 其余请求等这次刷新结束后读同一份结果——冷启动时也不让空表漏给监控。
func healthSnapshot(ctx context.Context) map[string]backendHealth {
	healthzMu.Lock()
	if !healthzAt.IsZero() && time.Since(healthzAt) < healthzTTL {
		s := healthzSnap
		healthzMu.Unlock()
		return s
	}
	if healthzFlight != nil {
		done := healthzFlight
		healthzMu.Unlock()
		select {
		case <-done:
		case <-ctx.Done(): // 调用方提前断开：不等了，直接用上一份快照
		}
		healthzMu.Lock()
		s := healthzSnap
		healthzMu.Unlock()
		return s
	}
	done := make(chan struct{})
	healthzFlight = done
	prev := healthzSnap
	healthzMu.Unlock()

	snap := mergeHealth(prev, refreshHealth())

	healthzMu.Lock()
	healthzSnap, healthzAt, healthzFlight = snap, time.Now(), nil
	healthzMu.Unlock()
	close(done)
	return snap
}

// refreshHealth 并发探测三后端，单轮总耗时不超过 healthzBudget。
func refreshHealth() map[string]backendHealth {
	ctx, cancel := context.WithTimeout(context.Background(), healthzBudget)
	defer cancel()
	var mu sync.Mutex
	out := make(map[string]backendHealth, len(backends))
	var wg sync.WaitGroup
	for _, b := range backends {
		wg.Add(1)
		go func(b *backend) {
			defer wg.Done()
			n, ok := probeBackend(ctx, b)
			mu.Lock()
			out[b.name] = backendHealth{Models: n, Reachable: ok}
			mu.Unlock()
		}(b)
	}
	// 预算用尽即返回已探到的部分（没探到的后端由 mergeHealth 沿用上一轮结果）。
	waited := make(chan struct{})
	go func() { wg.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-ctx.Done():
		log.Printf("[hub] healthz refresh hit %s budget", healthzBudget)
	}
	mu.Lock()
	defer mu.Unlock()
	snap := make(map[string]backendHealth, len(out))
	for k, v := range out {
		snap[k] = v
	}
	return snap
}

// probeBackend 探单个后端：先取模型目录（≤healthzModelCap，给兜底留预算），目录为空
// 再探 /healthz 判活（后端正常但没账号时目录本就为空）。返回 (模型数, 是否可达)。
func probeBackend(ctx context.Context, b *backend) (int, bool) {
	mctx, cancel := context.WithTimeout(ctx, healthzModelCap)
	ms := b.fetchModels(mctx)
	cancel()
	if len(ms) > 0 {
		// 只有探到目录才写表：旧实现无条件 setModels，一次探测失败就把 /v1/models 清空。
		b.setModels(ms)
		return len(ms), true
	}
	if probeOK(ctx, b) {
		return 0, true
	}
	go b.maybeRefreshModels() // 探失败：后台兜底重探（single-flight + 失败保旧表）
	return 0, false
}

// mergeHealth 合并新旧快照：本轮没探到（预算耗尽）或目录探测失败的后端，模型数沿用
// 上一次的非零值——目录不会因一次探测失败变 0；可达性一律用本轮结果，故障要如实
// 暴露给监控，不能靠"保留旧值"掩盖。
func mergeHealth(prev, fresh map[string]backendHealth) map[string]backendHealth {
	out := make(map[string]backendHealth, len(backends))
	for _, b := range backends {
		p := prev[b.name]
		f, ok := fresh[b.name]
		switch {
		case !ok:
			f = p
		case f.Models == 0 && p.Models > 0:
			f.Models = p.Models
		}
		out[b.name] = f
	}
	return out
}

// startupProbe 启动探一轮并把结果装进 healthz 首帧缓存：顺序探测最坏 3×15s，
// 会把 docker 健康检查拖到超时；并发 + ≤healthzBudget 后首帧 healthz 直接命中缓存。
func startupProbe() {
	snap := mergeHealth(nil, refreshHealth())
	healthzMu.Lock()
	healthzSnap, healthzAt = snap, time.Now()
	healthzMu.Unlock()
	for _, b := range backends {
		log.Printf("[hub] %s: %d models", b.name, b.modelsCount())
	}
}

// probeOK models 空也可能是后端正常但无账号——用 healthz 端点兜底判活。
// 走 modelsClient 复用连接池（旧实现每次 new client，探测时攒 TIME_WAIT）。
func probeOK(ctx context.Context, b *backend) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.target.String()+"/healthz", nil)
	if err != nil {
		return false
	}
	resp, err := modelsClient.Do(req)
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
