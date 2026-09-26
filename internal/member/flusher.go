package member

import "time"

// flushInterval 周期落盘间隔。member 消耗（窗口统计）此前只靠面板增删改与
// 停机时落盘——chat 记账只置脏不写盘，进程重启即丢自上次落盘以来的全部窗口
// 数据（2026-09-25 实测丢失 86 条记账）。与 pool 的 flusher 同构，30s 防抖。
const flushInterval = 30 * time.Second

// StartFlusher 启动周期落盘 goroutine（进程退出时由 main 的停机序再 Flush 一次）。
func (s *Store) StartFlusher() {
	go func() {
		t := time.NewTicker(flushInterval)
		defer t.Stop()
		for range t.C {
			s.Flush()
		}
	}()
}
