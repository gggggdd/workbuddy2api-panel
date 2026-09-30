package panel

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"workbuddy2api/internal/auth"
)

// cockpitAccount 映射 cockpit tools 导出格式的单个账号。
type cockpitAccount struct {
	ID             string `json:"id"`
	Email          string `json:"email"`
	UID            string `json:"uid"`
	Nickname       string `json:"nickname"`
	AccessToken    string `json:"access_token"`
	RefreshToken   string `json:"refresh_token"`
	TokenType      string `json:"token_type"`
	ExpiresAt      int64  `json:"expires_at"`
	Domain         string `json:"domain"`
	DosageNotify   string `json:"dosage_notify_code"`
	PaymentType    string `json:"payment_type"`
	Status         string `json:"status"`
	UsageUpdatedAt int64  `json:"usage_updated_at"`
	LastCheckin    int64  `json:"last_checkin_time"`
	CheckinStreak  int    `json:"checkin_streak"`
	CreatedAt      int64  `json:"created_at"`
	LastUsed       int64  `json:"last_used"`
}

// importCockpit 接收 cockpit tools 导出的 JSON 文件，批量导入账号到池中。
//
//	POST /panel/api/import/cockpit
//	Content-Type: multipart/form-data
//	Body: file=<json>
//
// 返回 {ok, total, imported, skipped, errors}。
func (p *Panel) importCockpit(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeErr(w, http.StatusBadRequest, "parse form: "+err.Error())
		return
	}
	file, _, err := r.FormFile("file")
	if err != nil {
		writeErr(w, http.StatusBadRequest, "missing file field: "+err.Error())
		return
	}
	defer file.Close()

	raw, err := io.ReadAll(file)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read file: "+err.Error())
		return
	}

	var accounts []cockpitAccount
	if err := json.Unmarshal(raw, &accounts); err != nil {
		// 非数组（如本机 cockpit 的 {account,auth} 单对象导出）：不报错，
		// 留空数组交给下方嵌套形分支统一解出。
		accounts = nil
	}
	if len(accounts) == 0 && !json.Valid(raw) {
		writeErr(w, http.StatusBadRequest, "invalid json")
		return
	}

	// 兼容本机 cockpit 的 auths 导出（嵌套形 {account:{uid,nickname}, auth:{accessToken,...},
	// camelCase）——这类条目 uid/access_token/refresh_token 全空，会整批 skipped。
	// 判定：扁平形数组至少一条带 uid 或 access_token；否则按嵌套形解。
	// 支持三种嵌套形态：{account,auth} 单对象、[{account,auth},...]、{accounts:[...]}。
	flattenNested := false
	{
		var probe []struct {
			UID         string `json:"uid"`
			AccessToken string `json:"access_token"`
		}
		if err := json.Unmarshal(raw, &probe); err != nil {
			flattenNested = true // 不是扁平数组 → 单对象或其他嵌套形态
		} else {
			has := false
			for _, a := range probe {
				if a.UID != "" || a.AccessToken != "" {
					has = true
					break
				}
			}
			flattenNested = !has // 数组存在但全部无 required 字段 → 嵌套数组
		}
	}
	if flattenNested {
		type nestedFile struct {
			Account struct {
				UID      string `json:"uid"`
				Nickname string `json:"nickname"`
			} `json:"account"`
			Auth struct {
				AccessToken  string `json:"accessToken"`
				RefreshToken string `json:"refreshToken"`
				ExpiresAt    int64  `json:"expiresAt"`
				Domain       string `json:"domain"`
			} `json:"auth"`
		}
		type nestedList struct {
			Items []nestedFile `json:"accounts"`
		}
		var flat []cockpitAccount
		appendNested := func(n nestedFile) {
			flat = append(flat, cockpitAccount{
				UID:          n.Account.UID,
				Nickname:     n.Account.Nickname,
				AccessToken:  n.Auth.AccessToken,
				RefreshToken: n.Auth.RefreshToken,
				ExpiresAt:    n.Auth.ExpiresAt,
				Domain:       n.Auth.Domain,
			})
		}
		// 形态 A：{account, auth} 单对象
		var single nestedFile
		if err := json.Unmarshal(raw, &single); err == nil && single.Account.UID != "" {
			appendNested(single)
		}
		// 形态 B：[{account, auth}, ...] 数组
		if len(flat) == 0 {
			var arr []nestedFile
			if err := json.Unmarshal(raw, &arr); err == nil && len(arr) > 0 {
				for _, n := range arr {
					appendNested(n)
				}
			}
		}
		// 形态 C：{accounts: [{account, auth}, ...]}（cockpit tools 整包导出）
		if len(flat) == 0 {
			var nl nestedList
			if err := json.Unmarshal(raw, &nl); err == nil && len(nl.Items) > 0 {
				for _, n := range nl.Items {
					appendNested(n)
				}
			}
		}
		if len(flat) > 0 {
			accounts = flat
			log.Printf("panel: cockpit import 检测到嵌套形 auths 导出，摊平 %d 条", len(flat))
		}
	}

	var total, imported, skipped int
	var errs []string

	for _, acc := range accounts {
		at := strings.TrimSpace(acc.AccessToken)
		rt := strings.TrimSpace(acc.RefreshToken)
		uid := strings.TrimSpace(acc.UID)
		// cockpit tools 新版导出无 uid 字段：从 access_token（JWT）的 sub claim
		// 解出真实账号 id；preferred_username 作昵称兜底。
		if uid == "" && at != "" {
			uid = jwtSub(at)
		}
		if uid == "" || at == "" || rt == "" {
			skipped++
			errs = append(errs, fmt.Sprintf("missing required fields (id=%s)", acc.ID))
			continue
		}
		if !validUID(uid) {
			skipped++
			errs = append(errs, fmt.Sprintf("invalid uid (id=%s)", acc.ID))
			continue
		}

		// 按 domain 推断 realm：workbuddy.ai 家族 → global，否则 cn。
		realm := auth.ResolveRealm("", acc.Domain)

		// cockpit tools 的 expires_at 为毫秒时间戳，转为秒。
		expiresAt := acc.ExpiresAt / 1000
		if expiresAt <= 0 {
			expiresAt = time.Now().Add(365 * 24 * time.Hour).Unix()
		}

		nickname := acc.Nickname
		if strings.TrimSpace(nickname) == "" {
			// cockpit 新版导出无 nickname：用 JWT 里的 preferred_username 或 email。
			nickname = jwtPreferredUsername(at)
		}
		if strings.TrimSpace(nickname) == "" {
			nickname = acc.Email
		}

		a := &auth.Auth{
			AccessToken:  at,
			RefreshToken: rt,
			ExpiresAt:    expiresAt,
			Domain:       acc.Domain,
			UID:          uid,
			Nickname:     nickname,
			FilePath:     filepath.Join(p.cfg.AuthDir, fmt.Sprintf("workbuddy-%s.json", uid)),
		}

		if realm == "global" {
			if _, err := auth.BackfillRealmFor(a, "global"); err != nil {
				skipped++
				errs = append(errs, fmt.Sprintf("uid=%s: set realm failed: %v", uid, err))
				continue
			}
		} else {
			_, _ = a.BackfillRealm()
		}

		if err := a.SaveAtomic(); err != nil {
			skipped++
			errs = append(errs, fmt.Sprintf("uid=%s: save auth failed: %v", uid, err))
			continue
		}

		p.cfg.Pool.Add(a)
		p.cfg.Pool.Revive(uid)

		// 顺带签到/激活（幂等；失败仅记日志，不阻断导入）。
		if realm == "global" {
			if activated, err := p.cfg.Upstream.GlobalCompleteRegistration(a); err != nil {
				log.Printf("panel: import global 注册激活 uid=%s: %v", uid, err)
			} else if activated {
				log.Printf("panel: import global 注册激活 uid=%s 完成", uid)
			}
			if claimed, err := p.cfg.Upstream.ClaimTrial(a); err != nil {
				log.Printf("panel: import global trial uid=%s: %v", uid, err)
			} else if claimed {
				log.Printf("panel: import global trial uid=%s 已领", uid)
			}
		} else {
			if err := p.cfg.Upstream.DailyCheckin(a); err != nil {
				log.Printf("panel: import checkin uid=%s: %v", uid, err)
			}
		}
		if rm, tt, err := p.cfg.Upstream.UserResource(a); err == nil {
			p.cfg.Pool.ReenableIfCredits(uid, rm, tt)
		}

		imported++
	}

	total = len(accounts)
	log.Printf("panel: cockpit import finished total=%d imported=%d skipped=%d", total, imported, skipped)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"total":    total,
		"imported": imported,
		"skipped":  skipped,
		"errors":   errs,
	})
}
