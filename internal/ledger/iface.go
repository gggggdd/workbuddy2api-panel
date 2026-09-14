package ledger

import "time"

// LedgerStore 接口化：scheduler / panel 依赖此接口，测试可注入假账本。
// （*Store 天然实现该接口。）
type LedgerStore interface {
	Append(e Entry)
	List(q Query) []Entry
	Summarize(q Query) []AccountSummary
	Totals(q Query) (inflow, outflow float64)
}

// DefaultPath 由 state_file 路径推导账本文件路径（同目录 ledger.json）。
func DefaultPath(stateFile string) string {
	if stateFile == "" {
		return "./data/ledger.json"
	}
	dir := stateFile
	for i := len(dir) - 1; i >= 0; i-- {
		if dir[i] == '/' {
			dir = dir[:i]
			break
		}
	}
	return dir + "/ledger.json"
}

// Now 便于测试注入时间（当前未用，预留）。
var _ = time.Now
