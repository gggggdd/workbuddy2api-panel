// hub_admin.go 三后端的管理面聚合：账号列表（带厂商归属）与 OAuth 登录代理。
//
// 账号聚合：workbuddy 走 panel 内部 pool（本进程不可达——panel 是独立服务，
// 这里走它的 /panel/api/overview）；trae 走 /admin/api/accounts（Bearer key）；
// qoder 走 console /api/accounts（session cookie，登录换会话）。
//
// OAuth 代理：trae /admin/api/login 发起扫码返回 login_url；qoder /api/oauth/start
// 同理。网关只做鉴权转换与结果拼装，登录动作本身仍发生在各后端与厂商之间。
package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var adminClient = &http.Client{Timeout: 15 * time.Second}

// backendReq 按后端各自的鉴权形态发起 GET。
func backendReq(b *backend, method, path string, body string, hdr map[string]string) (int, []byte) {
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, b.target.String()+path, rd)
	if err != nil {
		return 0, nil
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	if hdr == nil {
		if b.keyHdr == "x-api-key" {
			req.Header.Set("x-api-key", b.key)
		} else if b.key != "" {
			req.Header.Set("Authorization", "Bearer "+b.key)
		}
	}
	resp, err := adminClient.Do(req)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, raw
}

// hubAccounts 统一账号视图。
type hubAccount struct {
	Provider string `json:"provider"` // workbuddy / trae / qoder
	UID      string `json:"uid"`
	Nickname string `json:"nickname"`
	Realm    string `json:"realm,omitempty"`  // workbuddy: cn/global
	Enabled  *bool  `json:"enabled,omitempty"`
	Credits  any    `json:"credits,omitempty"` // trae/qoder 有；workbuddy 走 detail 页
	Status   string `json:"status,omitempty"`  // ok / cooling / disabled（各后端语义对齐后的小写）
	Detail   any    `json:"detail,omitempty"`  // 后端原始行（面板展开用）
}

// adminAccounts GET /hub/api/accounts → {accounts:[...], errors:{provider:msg}}
func adminAccounts(w http.ResponseWriter, r *http.Request) {
	out := []hubAccount{}
	errs := map[string]string{}
	wg := newSyncGroup()

	// workbuddy：panel overview 的 accounts。
	wg.Go(func() {
		wb := backends[0]
		code, raw := backendReq(wb, http.MethodGet, "/panel/api/overview", "", map[string]string{"Authorization": "Bearer " + wb.key})
		if code != 200 {
			errs["workbuddy"] = statusMsg(code)
			return
		}
		var d struct {
			Accounts []struct {
				UID      string `json:"uid"`
				Nickname string `json:"nickname"`
				Realm    string `json:"realm"`
				Disabled bool   `json:"disabled"`
				Credits  any    `json:"credits"`
				TokenUs  struct {
					RequestCount int64 `json:"request_count"`
				} `json:"token_usage"`
			} `json:"accounts"`
		}
		if json.Unmarshal(raw, &d) != nil {
			errs["workbuddy"] = "bad json"
			return
		}
		mu.Lock()
		for _, a := range d.Accounts {
			st := "ok"
			if a.Disabled {
				st = "disabled"
			}
			out = append(out, hubAccount{Provider: "workbuddy", UID: a.UID, Nickname: a.Nickname, Realm: a.Realm, Credits: a.Credits, Status: st})
		}
		mu.Unlock()
	})

	// trae：admin accounts。
	wg.Go(func() {
		tr := backends[1]
		code, raw := backendReq(tr, http.MethodGet, "/admin/api/accounts", "", map[string]string{"Authorization": "Bearer " + tr.key})
		if code != 200 {
			errs["trae"] = statusMsg(code)
			return
		}
		var d struct {
			Accounts []struct {
				UID      string `json:"uid"`
				Nickname string `json:"nickname"`
				Enabled  bool   `json:"enabled"`
				Disabled bool   `json:"disabled"`
				Cooling  bool   `json:"cooling"`
				Credits  any    `json:"credits"`
			} `json:"accounts"`
		}
		if json.Unmarshal(raw, &d) != nil {
			errs["trae"] = "bad json"
			return
		}
		mu.Lock()
		for _, a := range d.Accounts {
			st := "ok"
			if a.Disabled || !a.Enabled {
				st = "disabled"
			} else if a.Cooling {
				st = "cooling"
			}
			enabled := a.Enabled && !a.Disabled
			out = append(out, hubAccount{Provider: "trae", UID: a.UID, Nickname: a.Nickname, Enabled: &enabled, Credits: a.Credits, Status: st})
		}
		mu.Unlock()
	})

	// qoder：console 需要 session cookie——用 console password 换会话。
	wg.Go(func() {
		sid, err := qoderSession()
		if err != nil {
			errs["qoder"] = err.Error()
			return
		}
		code, raw := consoleReq("GET", "/api/accounts", "", sid)
		if code != 200 {
			errs["qoder"] = statusMsg(code)
			return
		}
		var list []struct {
			UID      string `json:"uid"`
			Nickname string `json:"nickname"`
			Region   string `json:"region"`
			Status   string `json:"status"`
		}
		if json.Unmarshal(raw, &list) != nil {
			// 空账号时是 []，合法。
			return
		}
		mu.Lock()
		for _, a := range list {
			enabled := a.Status == "active" || a.Status == ""
			out = append(out, hubAccount{Provider: "qoder", UID: a.UID, Nickname: a.Nickname, Realm: a.Region, Enabled: &enabled, Status: a.Status})
		}
		mu.Unlock()
	})

	wg.Wait()
	writeJSON(w, map[string]any{"accounts": out, "errors": errs})
}

// qoderSession 用 console password 登录换 session cookie（每次现取，24h 有效；
// qoder2api 重启后旧会话失效，现取最稳）。
func qoderSession() (string, error) {
	req, _ := http.NewRequest(http.MethodPost, qoderConsoleTarget+"/api/auth/login",
		strings.NewReader(`{"password":"`+qoderConsolePassword+`"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := adminClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	for _, c := range resp.Cookies() {
		if strings.HasPrefix(c.Name, "qoder2api_session") && c.Value != "" {
			return c.Value, nil
		}
	}
	return "", errNoSession
}

// adminOAuthStart POST /hub/api/oauth/start {"provider":"trae"|"qoder","region":"cn"|"global"}
// → 统一返回 {login_url, provider, session(内部状态 id)}。客户端拿到 login_url
// 打开授权；后续复用各后端自己的 wait/result 接口（trae /admin/api/login/result，
// qoder /api/oauth/wait）——网关代理这两个 wait 接口（见 adminOAuthWait）。
func adminOAuthStart(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Provider string `json:"provider"`
		Region   string `json:"region"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&req) != nil || req.Provider == "" {
		writeErr(w, http.StatusBadRequest, `body: {"provider":"trae"|"qoder"}`)
		return
	}
	switch req.Provider {
	case "trae":
		tr := backends[1]
		code, raw := backendReq(tr, http.MethodPost, "/admin/api/login", "{}", map[string]string{"Authorization": "Bearer " + tr.key, "Content-Type": "application/json"})
		proxyJSON(w, code, raw, "trae")
	case "qoder":
		sid, err := qoderSession()
		if err != nil {
			writeErr(w, http.StatusBadGateway, "qoder console login: "+err.Error())
			return
		}
		region := req.Region
		if region == "" {
			region = "cn"
		}
		code, raw := consoleReq("POST", "/api/oauth/start", `{"region":"`+region+`"}`, sid)
		proxyJSON(w, code, raw, "qoder")
	default:
		writeErr(w, http.StatusBadRequest, "unknown provider: "+req.Provider)
	}
}

// adminOAuthWait POST /hub/api/oauth/wait {"provider":..., ...} 透传各后端的
// 登录结果轮询（trae: /admin/api/login/result；qoder: /api/oauth/wait）。
// qoder 的 wait 需带原 login_id，body 原样透传。
func adminOAuthWait(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	var req struct {
		Provider string `json:"provider"`
	}
	if json.Unmarshal(raw, &req) != nil || req.Provider == "" {
		writeErr(w, http.StatusBadRequest, `body must include "provider"`)
		return
	}
	switch req.Provider {
	case "trae":
		tr := backends[1]
		code, respRaw := backendReq(tr, http.MethodPost, "/admin/api/login/result", string(raw), map[string]string{"Authorization": "Bearer " + tr.key, "Content-Type": "application/json"})
		proxyJSON(w, code, respRaw, "trae")
	case "qoder":
		sid, err := qoderSession()
		if err != nil {
			writeErr(w, http.StatusBadGateway, "qoder console login: "+err.Error())
			return
		}
		code, respRaw := consoleReq("POST", "/api/oauth/wait", string(raw), sid)
		proxyJSON(w, code, respRaw, "qoder")
	default:
		writeErr(w, http.StatusBadRequest, "unknown provider")
	}
}

// adminModels GET /hub/api/models?provider=trae|qoder → 该厂商原始模型目录（含
// context_length 等完整字段），供面板模型页与聚合视图分开取数。
func adminModels(w http.ResponseWriter, r *http.Request) {
	prov := r.URL.Query().Get("provider")
	var b *backend
	switch prov {
	case "trae":
		b = backends[1]
	case "qoder":
		b = backends[2]
	default:
		writeErr(w, http.StatusBadRequest, "provider must be trae|qoder")
		return
	}
	code, raw := backendReq(b, http.MethodGet, "/v1/models", "", nil)
	proxyJSON(w, code, raw, prov)
}

// consoleReq 打 qoder console（3588）的管理接口，带 session cookie。
func consoleReq(method, path, body, sid string) (int, []byte) {
	req, err := http.NewRequest(method, qoderConsoleTarget+path, strings.NewReader(body))
	if err != nil {
		return 0, nil
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Cookie", "qoder2api_session_3588="+sid)
	resp, err := adminClient.Do(req)
	if err != nil {
		return 0, nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, raw
}

func proxyJSON(w http.ResponseWriter, code int, raw []byte, provider string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if code >= 400 && len(raw) == 0 {
		io.WriteString(w, `{"error":"`+provider+` backend error `+itoa(code)+`"}`)
		return
	}
	w.Write(raw)
}

func statusMsg(code int) string {
	if code == 0 {
		return "unreachable"
	}
	return "status " + itoa(code)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := ""
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		digits = string(rune('0'+n%10)) + digits
		n /= 10
	}
	if neg {
		return "-" + digits
	}
	return digits
}

// ── 签到聚合 ──────────────────────────────────────────────────────────

// adminCheckin POST /hub/api/checkin {"provider":"trae"|"qoder"} 手动触发签到：
//   - trae 无手动签到 API（每日 9 点自动 + 3h 重试窗口），返回当前签到状态
//   - qoder POST /api/checkin 即时签到（每日 100 credits，需先有账号）
//
// 另 GET 同路径返回两家的签到状态（checked_in / enable / credits）。
func adminCheckin(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		out := map[string]any{}
		// trae 状态：admin credits 接口带 checked_in。
		tr := backends[1]
		code, raw := backendReq(tr, http.MethodGet, "/admin/api/credits", "", map[string]string{"Authorization": "Bearer " + tr.key})
		if code == 200 {
			var d struct {
				Accounts []map[string]any `json:"accounts"`
			}
			if json.Unmarshal(raw, &d) == nil {
				sts := []map[string]any{}
				for _, a := range d.Accounts {
					sts = append(sts, map[string]any{
						"uid": a["uid"], "nickname": a["nickname"],
						"checked_in": a["checked_in"], "checkin_credits": a["checkin_credits"],
					})
				}
				out["trae"] = map[string]any{"mode": "auto_daily_9am", "accounts": sts}
			}
		} else {
			out["trae"] = map[string]any{"error": statusMsg(code)}
		}
		// qoder 状态：settings 里 auto_checkin 开关 + 今日结果看 checkin 接口。
		if sid, err := qoderSession(); err == nil {
			code, raw := consoleReq("GET", "/api/settings", "", sid)
			if code == 200 {
				var d struct {
					Settings struct {
						AutoCheckin bool `json:"auto_checkin"`
					} `json:"settings"`
				}
				if json.Unmarshal(raw, &d) == nil {
					out["qoder"] = map[string]any{"auto_checkin": d.Settings.AutoCheckin, "mode": "auto_daily_10am"}
				}
			}
		}
		writeJSON(w, out)
		return
	}
	var req struct {
		Provider string `json:"provider"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&req) != nil || req.Provider == "" {
		writeErr(w, http.StatusBadRequest, `body: {"provider":"trae"|"qoder"}`)
		return
	}
	switch req.Provider {
	case "qoder":
		sid, err := qoderSession()
		if err != nil {
			writeErr(w, http.StatusBadGateway, "qoder console login: "+err.Error())
			return
		}
		code, raw := consoleReq("POST", "/api/checkin", "{}", sid)
		proxyJSON(w, code, raw, "qoder")
	case "trae":
		// trae 无手动触发端点：返回说明 + 当前状态。
		writeJSON(w, map[string]any{"ok": true, "note": "trae 签到为每日 9:00 自动（重试窗口 3h），无手动触发端点；可查 GET /hub/api/checkin 看状态"})
	default:
		writeErr(w, http.StatusBadRequest, "unknown provider")
	}
}

// adminOAuthComplete POST /hub/api/oauth/complete {"provider":"trae","callback_url":"http://127.0.0.1:18080/authorize?..."}
// Trae 专用：其回调打在用户本机 127.0.0.1:18080（本机模式设计），服务器部署收不到。
// 用户把浏览器最终跳转的完整 URL 粘回来，这里转发给 trae2api 的 /authorize 完成登录。
func adminOAuthComplete(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Provider    string `json:"provider"`
		CallbackURL string `json:"callback_url"`
	}
	if json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&req) != nil || req.CallbackURL == "" {
		writeErr(w, http.StatusBadRequest, `body: {"provider":"trae","callback_url":"..."}`)
		return
	}
	if req.Provider != "trae" {
		writeErr(w, http.StatusBadRequest, "only trae needs manual complete")
		return
	}
	tr := backends[1]
	u, err := url.Parse(req.CallbackURL)
	if err != nil || u.Path == "" {
		writeErr(w, http.StatusBadRequest, "callback_url parse failed")
		return
	}
	code, raw := backendReq(tr, http.MethodGet, u.RequestURI(), "", nil)
	proxyJSON(w, code, raw, "trae")
}
