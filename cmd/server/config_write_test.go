package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// TestWriteConfigFileAtomic 常规路径（非挂载点）走「先写 tmp 再 rename」：
// 内容正确，且不留下 .tmp 残留。
func TestWriteConfigFileAtomic(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "config.json")
	if err := os.WriteFile(fp, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	want := []byte(`{"listen":":7863"}`)
	if err := writeConfigFile(fp, want); err != nil {
		t.Fatalf("writeConfigFile: %v", err)
	}
	got, err := os.ReadFile(fp)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Errorf("content=%q want %q", got, want)
	}
	if _, err := os.Stat(fp + ".tmp"); !os.IsNotExist(err) {
		t.Errorf("tmp leftover: %v", err)
	}
}

// TestWriteConfigFileNewFile 目标不存在时也要能写（首次生成配置）。
func TestWriteConfigFileNewFile(t *testing.T) {
	dir := t.TempDir()
	fp := filepath.Join(dir, "new.json")
	if err := writeConfigFile(fp, []byte("x")); err != nil {
		t.Fatalf("writeConfigFile: %v", err)
	}
	if got, _ := os.ReadFile(fp); string(got) != "x" {
		t.Errorf("content=%q want x", got)
	}
}

// TestWriteConfigFileReadOnlyDir 不可恢复的失败必须报错，不能静默吞掉。
// （目录只读 → 写 tmp 就失败，与挂载点场景无关。）
func TestWriteConfigFileReadOnlyDir(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root 忽略目录权限，跳过")
	}
	dir := t.TempDir()
	ro := filepath.Join(dir, "ro")
	if err := os.Mkdir(ro, 0o500); err != nil {
		t.Fatal(err)
	}
	if err := writeConfigFile(filepath.Join(ro, "c.json"), []byte("x")); err == nil {
		t.Fatal("want error for read-only dir")
	}
}

// TestIsCrossDeviceOrBusy 只有 EBUSY（单文件 bind mount 的 rename 行为）与
// EXDEV（跨文件系统）才允许回退原地写入；其它错误必须上抛，避免把真实故障
// （如权限、磁盘满）伪装成"成功保存"。
func TestIsCrossDeviceOrBusy(t *testing.T) {
	if !isCrossDeviceOrBusy(syscall.EBUSY) {
		t.Error("EBUSY should allow fallback（Docker 单文件挂载）")
	}
	if !isCrossDeviceOrBusy(syscall.EXDEV) {
		t.Error("EXDEV should allow fallback")
	}
	if isCrossDeviceOrBusy(syscall.EACCES) {
		t.Error("EACCES must NOT allow fallback（权限问题应报错）")
	}
	if isCrossDeviceOrBusy(syscall.ENOSPC) {
		t.Error("ENOSPC must NOT allow fallback（磁盘满应报错）")
	}
	if isCrossDeviceOrBusy(os.ErrNotExist) {
		t.Error("ErrNotExist must NOT allow fallback")
	}
}
