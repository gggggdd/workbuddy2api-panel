package panel

import (
	"sync"
	"time"
)

// swrCache 面板慢接口的结果缓存（stale-while-revalidate）。
//
// 面板有两个接口的耗时来自"打上游"而非本机计算：
//   - /panel/api/packages  逐账号向上游实时查询（29 号 × 并发 3 ≈ 4s）
//   - /panel/api/models    并发探两路上游模型目录（≈0.7s）
//
// 这两个接口的数据变化频率都远低于访问频率（余额 5 分钟才刷一次、模型目录上游
// 侧自己缓存 1 小时），每次点开面板都真打一遍上游是纯浪费——而且 packages 那
// 种 4 秒的等待，用户感知就是"面板卡"。
//
// 语义（stale-while-revalidate）：
//   - 有未过期数据 → 立即返回（微秒级）
//   - 数据过期但存在 → **先立即返回旧数据**，同时后台异步刷新（下次就是新的）
//   - 从未拉过 → 同步拉一次（此时只能等，但只发生在首次/重启后）
//
// 并发安全：多个请求同时命中"过期"时，只有一个去刷（singleflight 语义，
// 用 mutex + 标志位实现，不引外部依赖）。
type swrCache struct {
	mu         sync.Mutex
	val        any
	fetched    time.Time
	ttl        time.Duration
	refreshing bool
	fetch      func() (any, error)
}

func newSWR(ttl time.Duration, fetch func() (any, error)) *swrCache {
	return &swrCache{ttl: ttl, fetch: fetch}
}

// get 返回缓存的数据；过期则后台刷新。force=true 时同步强制刷新
// （面板上的「重新查询」按钮用，绕过缓存）。
//
// 返回 (数据, 数据时间, 是否命中缓存)。从未拉到过数据时 data 为 nil。
func (c *swrCache) get(force bool) (any, time.Time, bool) {
	c.mu.Lock()
	if force || c.val == nil {
		// 首次或强制：同步拉（持锁拉取——首次只有一个请求，后来者等同一个结果，
		// 比各自打一遍上游好）。
		v, err := c.fetch()
		if err == nil {
			c.val, c.fetched = v, time.Now()
		} else if c.val == nil {
			c.mu.Unlock()
			return nil, time.Time{}, false
		}
		// 刷新失败且已有旧数据：保留旧值（宁旧勿空），不改 fetched，下次再试。
		out, at := c.val, c.fetched
		c.mu.Unlock()
		return out, at, false
	}
	stale := time.Since(c.fetched) >= c.ttl
	out, at := c.val, c.fetched
	if stale && !c.refreshing {
		c.refreshing = true
		go func() {
			v, err := c.fetch()
			c.mu.Lock()
			if err == nil {
				c.val, c.fetched = v, time.Now()
			}
			c.refreshing = false
			c.mu.Unlock()
		}()
	}
	c.mu.Unlock()
	return out, at, true
}

// age 数据年龄（供响应头 X-Data-Time 显示"数据时间"）。从未拉过返回零值时间。
func (c *swrCache) age() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fetched
}
