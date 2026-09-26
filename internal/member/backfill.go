package member

import (
	"encoding/json"
	"log"
	"os"
	"time"
)

// backfillFromLedger 从 ledger.json 的 entries 里恢复带 member 字段的消费记账，
// 补到 total_credit/requests（窗口桶太细不回填，回填后也早已滑出 5h/24h 窗口）。
// 幂等：只回填 ledger 中比成员 LastUsed 更新的条目（正常路径 Record 也会累加，
// 但崩溃/重启丢盘的那部分只有这里能补）。
// ledger 条目形态：{at, uid, nick, kind:"chat", delta:-x, model, member:"<id>"}。
// delta 为负（账号侧扣减），成员侧记正数消耗。
func (s *Store) BackfillFromLedger(ledgerPath string) {
	if ledgerPath == "" || s.path == "" {
		return
	}
	raw, err := os.ReadFile(ledgerPath)
	if err != nil {
		return
	}
	var ledger struct {
		Entries []struct {
			At     string  `json:"at"`
			Delta  float64 `json:"delta"`
			Member string  `json:"member"`
		} `json:"entries"`
	}
	if json.Unmarshal(raw, &ledger) != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	fixed := 0
	for _, e := range ledger.Entries {
		if e.Member == "" || e.Delta >= 0 {
			continue
		}
		at, err := time.Parse(time.RFC3339Nano, e.At)
		if err != nil {
			continue
		}
		for _, m := range s.members {
			if m.ID != e.Member {
				continue
			}
			// 只补比成员当前 LastUsed 更新的条目（避免与已落盘部分重复累加）。
			cutoff := time.Unix(m.LastUsed, 0)
			if m.LastUsed == 0 || at.After(cutoff) {
				m.TotalCredit += -e.Delta
				m.Requests++
				if at.Unix() > m.LastUsed {
					m.LastUsed = at.Unix()
				}
				s.dirty = true
				fixed++
			}
		}
	}
	if fixed > 0 {
		log.Printf("[member] backfill: 恢复 %d 条丢失的记账（源自 ledger）", fixed)
	}
}
