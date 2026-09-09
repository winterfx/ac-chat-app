package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseEnvLineReadsWhatAFileActuallyContains(t *testing.T) {
	cases := []struct {
		line       string
		key, value string
		ok         bool
	}{
		{line: "AC_DAEMON=http://host:7411", key: "AC_DAEMON", value: "http://host:7411", ok: true},
		{line: "  AC_DAEMON = http://host:7411  ", key: "AC_DAEMON", value: "http://host:7411", ok: true},
		{line: "export AC_DAEMON=http://host:7411", key: "AC_DAEMON", value: "http://host:7411", ok: true},
		{line: `AC_DAEMON="http://host:7411 "`, key: "AC_DAEMON", value: "http://host:7411 ", ok: true},
		{line: "AC_DAEMON=http://host:7411 # the staging one", key: "AC_DAEMON", value: "http://host:7411", ok: true},
		{line: "AC_EMPTY=", key: "AC_EMPTY", value: "", ok: true},
		{line: "# AC_DAEMON=http://host:7411"},
		{line: ""},
		{line: "not an assignment"},
		{line: "=orphan"},
	}
	for _, want := range cases {
		key, value, ok := parseEnvLine(want.line)
		if ok != want.ok || key != want.key || value != want.value {
			t.Errorf("parseEnvLine(%q) = %q, %q, %v; want %q, %q, %v",
				want.line, key, value, ok, want.key, want.value, want.ok)
		}
	}
}

// The environment is the deployment's own word on where the daemon is, and a
// file checked out beside the binary must not override it.
func TestLoadEnvFileLeavesAnAlreadySetVariableAlone(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("AC_DAEMON=http://from-file:7411\nAC_LISTEN=127.0.0.1:9999\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Setenv("AC_DAEMON", "http://from-environment:7411")

	if err := loadEnvFile(path); err != nil {
		t.Fatalf("loadEnvFile: %v", err)
	}
	if got := os.Getenv("AC_DAEMON"); got != "http://from-environment:7411" {
		t.Errorf("AC_DAEMON = %q, want the environment's value to survive", got)
	}
	if got := os.Getenv("AC_LISTEN"); got != "127.0.0.1:9999" {
		t.Errorf("AC_LISTEN = %q, want the file to supply what the environment did not", got)
	}
	t.Cleanup(func() { _ = os.Unsetenv("AC_LISTEN") })
}

// Running with no .env is the ordinary case for a deployment that passes
// everything as flags, not a misconfiguration.
func TestLoadEnvFileAcceptsAMissingFile(t *testing.T) {
	if err := loadEnvFile(filepath.Join(t.TempDir(), "absent")); err != nil {
		t.Fatalf("loadEnvFile on a missing file: %v", err)
	}
}

func TestSettingOrFallsBackOnBlank(t *testing.T) {
	t.Setenv("AC_BLANK", "   ")
	if got := settingOr("AC_BLANK", "fallback"); got != "fallback" {
		t.Errorf("settingOr on a blank value = %q, want the fallback", got)
	}
	t.Setenv("AC_SET", "chosen")
	if got := settingOr("AC_SET", "fallback"); got != "chosen" {
		t.Errorf("settingOr = %q, want the environment's value", got)
	}
}
