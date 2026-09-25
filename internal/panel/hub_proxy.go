package panel

import (
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
)

// registerHubProxy 把 /gw/ 前缀反代到本机 hub 聚合网关（默认 127.0.0.1:7860）。
//
// 背景：面板的跨厂商功能（Trae/Qoder 账号与模型聚合）需要访问 hub 的管理接口
// /hub/api/*。面板页面可能从两种入口打开——域名（Caddy 已把 /gw/* 直接反代到
// hub）或 IP:7863 直连（Caddy 不在链路上）。前端统一用同源相对路径 /gw/*，
// 因此 panel 自己也要能接住 /gw/*：存在 HUB_PROXY_TARGET 指向 hub 时注册
// 反代；未设置（hub 未部署）时不注册，前端 hubApi 会得到 404，与此前行为一致。
//
// 鉴权：hub 自带 HUB_API_KEY 校验，panel 透传 Authorization 头即可，不做二次
// 鉴权（/panel 的 withAuth 已保护了页面入口；hub key 本身就是跨厂商操作凭证）。
func NewHubProxy() http.Handler {
	target := os.Getenv("HUB_PROXY_TARGET")
	if target == "" {
		return nil
	}
	u, err := url.Parse(target)
	if err != nil {
		return nil
	}
	proxy := httputil.NewSingleHostReverseProxy(u)
	orig := proxy.Director
	proxy.Director = func(r *http.Request) {
		orig(r)
		r.Host = u.Host
		// 剥掉 /gw 前缀：panel 收到 /gw/hub/api/x，hub 只认 /hub/api/x。
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/gw")
		if r.URL.Path == "" {
			r.URL.Path = "/"
		}
	}
	proxy.FlushInterval = -1 // SSE 透传
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		io.WriteString(w, `{"error":"hub gateway unavailable"}`)
	}
	return proxy
}
