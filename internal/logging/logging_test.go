package logging

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestNewAcceptsValidLevels(t *testing.T) {
	tests := []struct {
		level string
		want  zapcore.Level
	}{
		{level: "debug", want: zapcore.DebugLevel},
		{level: "info", want: zapcore.InfoLevel},
		{level: "warn", want: zapcore.WarnLevel},
		{level: "error", want: zapcore.ErrorLevel},
		{level: "INFO", want: zapcore.InfoLevel},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			log, err := New(tt.level)
			require.NoError(t, err)
			require.NotNil(t, log)

			assert.True(t, log.Core().Enabled(tt.want))
			if tt.want > zapcore.DebugLevel {
				assert.False(t, log.Core().Enabled(tt.want-1))
			}
		})
	}
}

func TestNewRejectsUnknownLevel(t *testing.T) {
	log, err := New("chatty")

	require.Error(t, err)
	assert.Nil(t, log)
	assert.Contains(t, err.Error(), "parse log level")
}
