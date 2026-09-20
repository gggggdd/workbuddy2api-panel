// signin 一次性批量签到工具：遍历 ./auths/workbuddy-*.json 全部账号，
// 自动 RefreshToken（过期时），逐个调 daily-checkin，顺手查余额。
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"workbuddy2api/internal/auth"
	"workbuddy2api/internal/upstream"
)

type row struct {
	file     string
	uid      string
	nick     string
	status   string // OK | ALREADY | FAIL | AUTH_INVALID | LOAD_ERR
	detail   string
	remain   int64
	hasQuota bool
}

func main() {
	dir := "auths"
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	files, err := filepath.Glob(filepath.Join(dir, "workbuddy-*.json"))
	if err != nil || len(files) == 0 {
		fmt.Fprintf(os.Stderr, "no auth files in %s\n", dir)
		os.Exit(1)
	}
	sort.Strings(files)
	up := upstream.New()

	var rows []row
	okN, alreadyN, failN := 0, 0, 0
	for _, f := range files {
		r := row{file: filepath.Base(f)}
		raw, err := os.ReadFile(f)
		if err != nil {
			r.status, r.detail = "LOAD_ERR", err.Error()
			rows = append(rows, r)
			failN++
			continue
		}
		a, err := auth.Parse(raw)
		if err != nil {
			r.status, r.detail = "LOAD_ERR", err.Error()
			rows = append(rows, r)
			failN++
			continue
		}
		a.FilePath = f
		r.uid, r.nick = a.UID, a.Nickname

		// refresh 过期 token
		if a.NeedsRefresh(2 * 3600) {
			if err := up.RefreshToken(a); err != nil {
				if ue, ok := err.(*upstream.Error); ok && ue.Kind == upstream.ErrSessionDead {
					r.status = "AUTH_INVALID"
				} else {
					r.status = "FAIL"
				}
				r.detail = "refresh: " + short(err.Error())
				rows = append(rows, r)
				failN++
				continue
			}
			// refresh 后写回文件（权限问题已修复）；落盘失败必须暴露，否则重启回旧 token
			if err := a.SaveAtomic(); err != nil {
				log.Printf("signin %s save: %v", a.UID, err)
			}
		}

		err = up.DailyCheckin(a)
		// DailyCheckin 重复调用返回 code!=0（已签到）——归 ALREADY 而非 FAIL。
		r.status = checkinStatusOf(err)
		switch r.status {
		case "OK":
			okN++
		case "ALREADY":
			r.detail = short(err.Error())
			alreadyN++
		default:
			r.detail = short(err.Error())
			failN++
		}
		// 顺手查余额
		if remain, _, qerr := up.UserResource(a); qerr == nil {
			r.remain, r.hasQuota = remain, true
		}
		rows = append(rows, r)
	}

	// 报告
	fmt.Printf("uid                                  | nick        | status       | remain | detail\n")
	fmt.Printf("-------------------------------------+-------------+--------------+--------+------------------------------\n")
	for _, r := range rows {
		remain := "-"
		if r.hasQuota {
			remain = fmt.Sprintf("%d", r.remain)
		}
		fmt.Printf("%-36s | %-11s | %-12s | %-6s | %s\n",
			trunc(r.uid, 36), trunc(r.nick, 11), r.status, remain, r.detail)
	}
	fmt.Printf("\ntotal=%d ok=%d already=%d fail=%d\n", len(rows), okN, alreadyN, failN)
}

// checkinStatusOf 把签到结果 error 映射为状态字：OK / ALREADY / FAIL。
//
// 抽成纯函数便于单测（不依赖网络与账号），调用点只负责计数与详情。
func checkinStatusOf(err error) string {
	switch {
	case err == nil:
		return "OK"
	case isAlready(err):
		return "ALREADY"
	default:
		return "FAIL"
	}
}

// 已签判定：把「今天已签到 / 活动未开启 / 已过期 / session inactive」这类
// 幂等或不适用情形与真实失败区分开——global 账号多数没有签到体系，混判 FAIL
// 会让手跑 signin 时成片标红，掩盖真实故障。
//
// 分两层，因为英文短词区分度低：
//   - 业务 *upstream.Error（Msg 来自上游 JSON）：「已签到/已过期/功能未开启」
//     中文关键词 + 「already/inactive/checkin」英文短词全参与匹配。
//   - 裸 error（网络/代理/TLS 文本）：只认「已签到」这类中文关键词，不认英文
//     短词——"bind: address already in use" 是 TCP 绑定失败的常见文本，若判成
//     已签到会在停机补签时把真实故障咽掉。
func isAlready(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "已签到") || strings.Contains(s, "已过期") ||
		strings.Contains(s, "功能未开启") || strings.Contains(s, "未开启") {
		return true
	}
	var ue *upstream.Error
	if errors.As(err, &ue) {
		// 业务错误：英文短词才是有效信号。
		return strings.Contains(s, "already") ||
			strings.Contains(s, "inactive") ||
			strings.Contains(s, "checkin") ||
			strings.Contains(s, "code=10001") ||
			strings.Contains(s, "code=14001") ||
			strings.Contains(s, "code=20003")
	}
	return false
}

func trunc(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func short(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 60 {
		return s[:60]
	}
	return s
}
