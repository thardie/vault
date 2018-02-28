package logformat

import (
	"os"
	"strconv"
	"strings"
	"sync"

	hclog "github.com/hashicorp/go-hclog"
)

const (
	defaultTimeFormat = "2006/01/02 15:04:05.000000"
)

// TODO this method name is too specific
func NewVaultHCLogger(level int) hclog.Logger {
	opts := &hclog.LoggerOptions{
		Name: "vault",
		// TODO - do the numbers being passed in match the ones used
		// in this package? Further, is there a better way to do this
		// than this lame string conversion?
		Level:           hclog.LevelFromString(strconv.Itoa(level)),
		Output:          "TODO", // TODO how is it determining where to write logs to?
		Mutex:           &sync.Mutex{},
		JSONFormat:      useJSONFormat(),
		IncludeLocation: true, // TODO just kinda picked this
		TimeFormat:      defaultTimeFormat,
	}
	return hclog.New(opts)
}

func logFormat() string {
	logFormat := os.Getenv("VAULT_LOG_FORMAT")
	if logFormat == "" {
		logFormat = os.Getenv("LOGXI_FORMAT")
	}
	return logFormat
}

// TODO proper naming according to Go conventions?
func useJSONFormat() bool {
	// TODO don't hard-code these here AND in logxi.go
	logFormat := logFormat()
	switch strings.ToLower(logFormat) {
	case "json", "vault_json", "vault-json", "vaultjson":
		return true
	default:
		return false
	}
}
