package telemetry

import (
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/log/global"
)

func GetLogger(name string) *slog.Logger {
	return otelslog.NewLogger(name, otelslog.WithLoggerProvider(global.GetLoggerProvider()))
}
