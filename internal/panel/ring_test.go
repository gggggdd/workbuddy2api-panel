package panel

import (
	"strings"
	"testing"
	"time"
)

func TestClassifyLine(t *testing.T) {
	cases := map[string]string{
		"| #001 | glm-5.2 | stream | 200 | uid=c8a3e793 | TTFB=120ms |": ChChat,
		"school c8a3e793: ★ 分享任务完成":                                     ChTask,
		"streak-bonus 5c162cc9: 🎊 新手礼包 +100c":                           ChTask,
		"blackcat c8a3e793: 完成 3 次夜间对话":                                 ChTask,
		"checkin 5c162cc9: 已签到":                                         ChTask,
		"panel: 任务动作 uid=x code=chat_5":                                 ChTask,
		"panel: 队列启动：6 项（并发 2）":                                         ChTask,
		"panel: revive uid=x":                                           ChSys,
		// 漏判回归：冒号形态的 blackcat 跳过行 + 任务动作类面板日志
		"blackcat: 当前不在 23:00–08:00 计数窗口，跳过":         ChTask,
		"panel: 接受任务 uid=c8a3e793 codes=[x]":         ChTask,
		"panel: 领取任务奖励 uid=c8a3e793 code=x +10分 +1能": ChTask,
		"panel: 全部接受 uid=c8a3e793 接受=3 失败=0":         ChTask,
		"panel: 定时成长任务队列已启动（6 项）":                    ChTask,
		"workbuddy2api listening on :7863":           ChSys,
		"scheduler: 余额后台刷新每 5m0s":                    ChSys,
	}
	for line, want := range cases {
		if got := classifyLine(line); got != want {
			t.Errorf("classifyLine(%q)=%q want %q", line, got, want)
		}
	}
}

func TestRingWriteStripsTimestamp(t *testing.T) {
	r := NewRing(4)
	if _, err := r.Write([]byte("2026/09/14 00:12:34 school x: done\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Write([]byte("| #002 | glm | stream | 200 | ok |")); err != nil {
		t.Fatal(err)
	}
	es := r.Snapshot()
	if len(es) != 2 {
		t.Fatalf("entries=%d want 2", len(es))
	}
	if strings.HasPrefix(es[0].Text, "2026/") {
		t.Errorf("timestamp not stripped: %q", es[0].Text)
	}
	if es[0].Ch != ChTask || es[1].Ch != ChChat {
		t.Errorf("channels: %q %q", es[0].Ch, es[1].Ch)
	}
	if time.Since(es[0].TS) > 5*time.Second {
		t.Errorf("stale ts: %v", es[0].TS)
	}
}

// TestRingProtectsTaskFromChatFlood 对话洪水不得冲掉任务日志。
//
// 回归护栏：此前所有频道共用一条严格 FIFO 队列，chat 每请求一行、产量远大于
// 任务动作，任务行（签到 / 猫猫旅行 / 抽奖结果）在几百行内就被冲掉，
// 用户报告「签到和猫猫旅行都不在日志显示了」。
func TestRingProtectsTaskFromChatFlood(t *testing.T) {
	r := NewRing(10)
	if _, err := r.Write([]byte("checkin 5c162cc9: 已签到\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Write([]byte("travel 5c162cc9: claim ok reward=7\n")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		if _, err := r.Write([]byte("| #001 | glm-5.2 | stream | 200 | ok |\n")); err != nil {
			t.Fatal(err)
		}
	}
	es := r.Snapshot()
	if len(es) != 10 {
		t.Fatalf("entries=%d want 10（容量必须守住）", len(es))
	}
	task := 0
	for _, e := range es {
		if e.Ch == ChTask {
			task++
		}
	}
	if task != 2 {
		t.Errorf("task entries=%d want 2（任务日志被对话洪水冲掉了）", task)
	}
}

// TestRingTaskDoesNotStarveChat task 不得独占缓冲：超过 cap/2 后要让位给对话日志。
func TestRingTaskDoesNotStarveChat(t *testing.T) {
	r := NewRing(10)
	for i := 0; i < 40; i++ {
		if _, err := r.Write([]byte("checkin 5c162cc9: 已签到\n")); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 40; i++ {
		if _, err := r.Write([]byte("| #001 | glm-5.2 | stream | 200 | ok |\n")); err != nil {
			t.Fatal(err)
		}
	}
	es := r.Snapshot()
	if len(es) != 10 {
		t.Fatalf("entries=%d want 10", len(es))
	}
	var task, chat int
	for _, e := range es {
		switch e.Ch {
		case ChTask:
			task++
		case ChChat:
			chat++
		}
	}
	if task > 5 {
		t.Errorf("task entries=%d want <=5（cap/2 上限失效）", task)
	}
	if chat == 0 {
		t.Error("chat entries=0：对话日志被任务日志挤空了")
	}
}
