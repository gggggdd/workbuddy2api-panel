// credit.go — WorkBuddy 积分查询（全部账号 + 总计），JSON 输出到 stdout。
//
// 用法:
//
//	go run ./cmd/credit        # 或编译后 ./credit
//
// 输出结构:
//
//	{"service":"workbuddy","ts":N,
//	 "total":{"remain":N,"used":N,"size":N,"accounts":N,"ok":N,"failed":N},
//	 "accounts":[{"uid","nickname","remain","used","size","packages","ok","error?"}]}
//
// 接口与聚合逻辑：POST codebuddy.cn/v2/billing/meter/get-user-resource，聚合所有 package 的
// Cycle* 字段，TotalDosage 作 size 下限。
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"workbuddy2api/internal/auth"
	"workbuddy2api/internal/upstream"
)

const billingBaseCN = "https://www.codebuddy.cn"

// billingBaseGlobal 国际版计费域。global 账号打 CN 域会得到 401：
// www.codebuddy.cn 不认 workbuddy.ai 的 token（实测 401，workbuddy.ai 同 token 为 code=0）。
const billingBaseGlobal = "https://www.workbuddy.ai"

// billingBaseFor 按账号 realm 选择计费域。
//
// 判定口径与 internal/auth.Realm() 一致：显式 realm=global 或 domain 落在
// workbuddy.ai 家族，都按国际版处理。cmd/credit 早先对所有账号硬编码 CN 域，
// 导致 global 账号余额查询恒返回 401（面板显示的是池内缓存值，不是实时的）。
func billingBaseFor(af *authFile) string {
	if strings.EqualFold(strings.TrimSpace(af.Auth.Realm), "global") {
		return billingBaseGlobal
	}
	d := strings.ToLower(strings.TrimSpace(af.Auth.Domain))
	if d == "workbuddy.ai" || strings.HasSuffix(d, ".workbuddy.ai") {
		return billingBaseGlobal
	}
	return billingBaseCN
}

type authFile struct {
	Auth struct {
		AccessToken string `json:"accessToken"`
		Domain      string `json:"domain"`
		Realm       string `json:"realm"`
	} `json:"auth"`
	Account struct {
		UID          string `json:"uid"`
		EnterpriseID string `json:"enterpriseId"`
		Nickname     string `json:"nickname"`
	} `json:"account"`
}

type accountResult struct {
	UID      string `json:"uid"`
	Nickname string `json:"nickname"`
	Remain   *int64 `json:"remain"`
	Used     *int64 `json:"used"`
	Size     *int64 `json:"size"`
	Packages int    `json:"packages,omitempty"`
	OK       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
}

type resourcePackage struct {
	CapacityRemain      int64 `json:"CapacityRemain"`
	CapacityUsed        int64 `json:"CapacityUsed"`
	CapacitySize        int64 `json:"CapacitySize"`
	CycleCapacityRemain int64 `json:"CycleCapacityRemain"`
	CycleCapacityUsed   int64 `json:"CycleCapacityUsed"`
	CycleCapacitySize   int64 `json:"CycleCapacitySize"`
}

// packageRemainUsed 与 billing.go:203-258 一致
func packageRemainUsed(a resourcePackage) (remain, used, size int64) {
	if a.CycleCapacitySize > 0 {
		remain = a.CycleCapacityRemain
		size = a.CycleCapacitySize
		if remain < 0 {
			remain = 0
		}
		if remain > size {
			remain = size
		}
		used = size - remain
		if a.CycleCapacityUsed > used {
			used = a.CycleCapacityUsed
			if size >= used {
				remain = size - used
			}
		}
		return remain, used, size
	}
	remain = a.CapacityRemain
	used = a.CapacityUsed
	size = a.CapacitySize
	if used == 0 && size > remain {
		used = size - remain
	}
	return remain, used, size
}

// collect 遍历 dir 下的 workbuddy-*.json，逐账号查积分构成。
//
// 接受 *upstream.Client 而非自建 http.Client：realm 路由（global 优先无 /v2 的
// billing 域、404 回落 /v2；CN 单走 /v2）由 internal/upstream.billingMeterPaths
// 统一决定，工具侧不再重复实现一套，避免两处口径漂移。
//
// 无 token / 损坏文件由 auth.Parse 拒收 → 静默跳过、不发请求（与 signin/trial
// 对损坏文件的处理一致）。
func collect(dir string, up *upstream.Client) []accountResult {
	files, _ := filepath.Glob(filepath.Join(dir, "workbuddy-*.json"))
	sort.Strings(files)

	accounts := make([]accountResult, 0, len(files))
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		a, err := auth.Parse(raw)
		if err != nil {
			continue // 无 accessToken / 解析失败：跳过
		}
		res := accountResult{UID: a.UID, Nickname: a.Nickname}
		packs, remain, size, err := up.CreditPackages(a)
		if err != nil {
			res.Error = err.Error()
		} else {
			used := size - remain
			if used < 0 {
				used = 0
			}
			res.Remain, res.Used, res.Size = &remain, &used, &size
			res.Packages, res.OK = len(packs), true
		}
		accounts = append(accounts, res)
		time.Sleep(200 * time.Millisecond)
	}
	return accounts
}

func main() {
	pretty := len(os.Args) > 1 && os.Args[1] == "-pretty"
	authDir := "./auths"
	if v := os.Getenv("WB2A_AUTH_DIR"); v != "" {
		authDir = v
	}
	// realm 路由交给 upstream.Client（billingMeterPaths 统一决定双路径/单路径）。
	up := &upstream.Client{
		ChatBaseCN:        billingBaseCN,
		BillingBaseCN:     billingBaseCN,
		BillingBaseGlobal: billingBaseGlobal,
		GlobalEnabled:     true,
	}
	accounts := collect(authDir, up)

	var totalRemain, totalUsed, totalSize int64
	okCount := 0
	for _, a := range accounts {
		if a.OK {
			okCount++
			if a.Remain != nil {
				totalRemain += *a.Remain
			}
			if a.Used != nil {
				totalUsed += *a.Used
			}
			if a.Size != nil {
				totalSize += *a.Size
			}
		}
	}
	out := map[string]any{
		"service": "workbuddy",
		"ts":      time.Now().Unix(),
		"total": map[string]any{
			"remain":   totalRemain,
			"used":     totalUsed,
			"size":     totalSize,
			"accounts": len(accounts),
			"ok":       okCount,
			"failed":   len(accounts) - okCount,
		},
		"accounts": accounts,
	}
	if pretty {
		printPretty(accounts, totalRemain, totalUsed, totalSize, okCount)
		return
	}
	raw, _ := json.Marshal(out)
	fmt.Println(string(raw))
}

// printPretty 人类可读日报：四行汇总，无账号明细。
func printPretty(accounts []accountResult, totalRemain, totalUsed, totalSize int64, okCount int) {
	withBalance := 0
	var failed []string
	for _, a := range accounts {
		if a.OK && a.Remain != nil && *a.Remain > 0 {
			withBalance++
		}
		if !a.OK {
			name := a.Nickname
			if name == "" && len(a.UID) >= 8 {
				name = a.UID[:8]
			}
			failed = append(failed, name+" "+a.Error)
		}
	}
	pct := int64(0)
	if totalSize > 0 {
		pct = totalRemain * 100 / totalSize
	}
	fmt.Printf("📊 WorkBuddy 积分日报\n")
	fmt.Printf("账号: %d/%d\n", withBalance, len(accounts))
	fmt.Printf("总计: %d/%d\n", totalRemain, totalSize)
	fmt.Printf("剩余: %d%%\n", pct)
	for _, f := range failed {
		fmt.Printf("⚠️ %s\n", f)
	}
}
