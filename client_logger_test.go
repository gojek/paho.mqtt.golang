package mqtt

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

type mockLogger struct {
	out *bytes.Buffer
}

func (l *mockLogger) Println(v ...interface{}) {
	fmt.Fprintln(l.out, v...)
}

func (l *mockLogger) Printf(format string, v ...interface{}) {
	fmt.Fprintf(l.out, format, v...)
}

func TestNewClientLogger(t *testing.T) {
	buf := &bytes.Buffer{}
	mock := &mockLogger{out: buf}

	logger := NewClientLogger("test-client", mock, mock, mock, mock)

	logger.Error().Println("error log")
	logger.Critical().Printf("critical log: %d", 42)
	logger.Warn().Println("warn log")
	logger.Debug().Printf("debug log: %s", "verbose")

	logs := buf.String()

	for _, keyword := range []string{"[test-client]"} {
		if !strings.Contains(logs, keyword) {
			t.Errorf("expected logs to contain prefix %q, got: %s", keyword, logs)
		}
	}

	expectedSubstrings := []string{
		"[test-client] error log",
		"[test-client] critical log: 42",
		"[test-client] warn log",
		"[test-client] debug log: verbose",
	}
	for _, substr := range expectedSubstrings {
		if !strings.Contains(logs, substr) {
			t.Errorf("expected log to contain %q, got: %s", substr, logs)
		}
	}
}
