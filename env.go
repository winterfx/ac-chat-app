package main

import (
	"bufio"
	"os"
	"strings"
)

// envFile is where this app looks for its settings when they are not given on
// the command line. A file rather than only flags because the daemon address
// is deployment state, not something anyone should retype per run.
const envFile = ".env"

// loadEnvFile reads KEY=VALUE lines into the process environment, leaving any
// variable that is already set alone: what the environment says wins over what
// a checked-out file happens to contain.
//
// A missing file is not an error. It is the ordinary case for a run that
// configures itself entirely through flags.
func loadEnvFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		key, value, ok := parseEnvLine(scanner.Text())
		if !ok {
			continue
		}
		if _, taken := os.LookupEnv(key); taken {
			continue
		}
		if err := os.Setenv(key, value); err != nil {
			return err
		}
	}
	return scanner.Err()
}

// parseEnvLine reads one KEY=VALUE line, reporting false for blanks, comments
// and anything without a name. Values may be quoted, which is how a value with
// trailing spaces or a '#' is written.
func parseEnvLine(line string) (string, string, bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false
	}
	line = strings.TrimPrefix(line, "export ")
	key, value, found := strings.Cut(line, "=")
	if !found {
		return "", "", false
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", "", false
	}
	value = strings.TrimSpace(value)
	if len(value) >= 2 {
		if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
			return key, value[1 : len(value)-1], true
		}
	}
	// An unquoted value ends at a comment, so "http://host # note" is an
	// address rather than an address with a note glued to it.
	if cut := strings.Index(value, " #"); cut >= 0 {
		value = strings.TrimSpace(value[:cut])
	}
	return key, value, true
}

// settingOr returns the environment's value for name, or fallback when it is
// unset or blank.
func settingOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
