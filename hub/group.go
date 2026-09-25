package main

import "sync"

// hub_admin 用的极简并发组与共享锁（避免为三路并行探测引入 x/sync 依赖）。
var mu sync.Mutex

type syncGroup struct {
	wg   sync.WaitGroup
	once sync.Once
}

func newSyncGroup() *syncGroup { return &syncGroup{} }

func (g *syncGroup) Go(f func()) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		f()
	}()
}

func (g *syncGroup) Wait() { g.once.Do(func() { g.wg.Wait() }) }
