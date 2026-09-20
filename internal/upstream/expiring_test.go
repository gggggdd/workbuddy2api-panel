package upstream

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"

	"workbuddy2api/internal/auth"
)

// mkDetailedResp 构造 get-user-resource 响应。
//
// 到期时间字段用 CycleEndTime：上游实测 CN/global 两域套餐字段全集都没有
// PackageEndTime，拿它当判据 expiring 恒为 0（该 bug 已随上游同步修正）。
func mkDetailedResp(accounts string) *http.Response {
	body := `{"code":0,"data":{"Response":{"Data":{"TotalCount":1,"Accounts":[` + accounts + `]}}}}`
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(bytes.NewReader([]byte(body))),
		Header:     make(http.Header),
	}
}

// TestUserResourceDetailedSplitsExpiring 快过期分桶：窗口内的包计入 expiring，
// 窗口外计入长期（remain - expiring）。断言用签名给出的三个返回值。
func TestUserResourceDetailedSplitsExpiring(t *testing.T) {
	now := time.Now()
	in3d := now.Add(3 * 24 * time.Hour).Format(packageEndLayout)
	in30d := now.Add(30 * 24 * time.Hour).Format(packageEndLayout)
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return mkDetailedResp(
			`{"PackageName":"奖励包","CycleEndTime":"` + in3d + `","CycleCapacitySize":1500,"CycleCapacityRemain":1200,"CycleCapacityUsed":300},` +
				`{"PackageName":"周期包","CycleEndTime":"` + in30d + `","CycleCapacitySize":500,"CycleCapacityRemain":300,"CycleCapacityUsed":200}`), nil
	})
	a := &auth.Auth{AccessToken: "at", UID: "u1"}
	remain, total, expiring, err := c.UserResourceDetailed(a, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("detailed: %v", err)
	}
	if remain != 1500 {
		t.Errorf("remain=%d want 1500", remain)
	}
	if expiring != 1200 {
		t.Errorf("expiring=%d want 1200 (3天内过期的奖励包)", expiring)
	}
	if stable := remain - expiring; stable != 300 {
		t.Errorf("stable=%d want 300 (30天后才过期的周期包)", stable)
	}
	// total 是总额度（各包 size 之和 = 1500+500），与 remain（可用余额）不同——
	// 面板百分比与快过期占比都用 total 做分母，故这两个值必须分别校验。
	if total != 2000 {
		t.Errorf("total=%d want 2000 (1500+500 两包额度之和)", total)
	}
}

// TestUserResourceDetailedNoWindowAllStable soon<=0：禁用分桶，expiring 恒 0
// （向后兼容旧行为）——调用方据此退化为纯总量口径。
func TestUserResourceDetailedNoWindowAllStable(t *testing.T) {
	in3d := time.Now().Add(3 * 24 * time.Hour).Format(packageEndLayout)
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return mkDetailedResp(
			`{"PackageName":"p","CycleEndTime":"` + in3d + `","CycleCapacitySize":100,"CycleCapacityRemain":80,"CycleCapacityUsed":20}`), nil
	})
	remain, _, expiring, err := c.UserResourceDetailed(&auth.Auth{AccessToken: "at"}, 0)
	if err != nil {
		t.Fatalf("detailed: %v", err)
	}
	if expiring != 0 || remain != 80 {
		t.Errorf("remain=%d expiring=%d want {80 0} when soon=0", remain, expiring)
	}
}

// TestUserResourceDetailedMissingEndTimeStable 无到期时间字段：保守归长期，
// 不误标快过期去插队消费。
func TestUserResourceDetailedMissingEndTimeStable(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return mkDetailedResp(
			`{"PackageName":"p","CycleCapacitySize":100,"CycleCapacityRemain":80,"CycleCapacityUsed":20}`), nil
	})
	remain, _, expiring, err := c.UserResourceDetailed(&auth.Auth{AccessToken: "at"}, 7*24*time.Hour)
	if err != nil {
		t.Fatalf("detailed: %v", err)
	}
	if expiring != 0 || remain != 80 {
		t.Errorf("remain=%d expiring=%d want {80 0} for missing end time", remain, expiring)
	}
}

// TestUserResourceBackwardCompat 旧 UserResource 签名与总量口径不变。
func TestUserResourceBackwardCompat(t *testing.T) {
	c := testClient(func(r *http.Request) (*http.Response, error) {
		return mkDetailedResp(
			`{"PackageName":"p","CycleCapacitySize":100,"CycleCapacityRemain":80,"CycleCapacityUsed":20}`), nil
	})
	remain, _, err := c.UserResource(&auth.Auth{AccessToken: "at"})
	if err != nil || remain != 80 {
		t.Errorf("UserResource remain=%d err=%v, want 80/nil", remain, err)
	}
}
