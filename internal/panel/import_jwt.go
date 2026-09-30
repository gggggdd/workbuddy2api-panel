package panel

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

// jwtSub 从 access_token（JWT）的 payload 里解出 sub claim——cockpit tools
// 新版导出不再带 uid 字段，真实账号 id 在 token 里。payload 是 URL-safe
// base64，无签名校验（这里只读字段，不验证来源——token 有效性由上游验证）。
func jwtSub(accessToken string) string {
	parts := strings.Split(accessToken, ".")
	if len(parts) != 3 {
		return ""
	}
	p := parts[1]
	if pad := len(p) % 4; pad != 0 {
		p += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.URLEncoding.DecodeString(p)
	if err != nil {
		return ""
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if json.Unmarshal(raw, &claims) != nil {
		return ""
	}
	return strings.TrimSpace(claims.Sub)
}

// jwtPreferredUsername 解出登录用户名（ preferred_username），作昵称兜底。
func jwtPreferredUsername(accessToken string) string {
	parts := strings.Split(accessToken, ".")
	if len(parts) != 3 {
		return ""
	}
	p := parts[1]
	if pad := len(p) % 4; pad != 0 {
		p += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.URLEncoding.DecodeString(p)
	if err != nil {
		return ""
	}
	var claims struct {
		PreferredUsername string `json:"preferred_username"`
	}
	if json.Unmarshal(raw, &claims) != nil {
		return ""
	}
	return strings.TrimSpace(claims.PreferredUsername)
}
