/*
Source: https://opentelemetry.io/docs/languages/go/getting-started/
*/
package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

func SetupOTelSDK(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	var shutdownFuncs []func(context.Context) error
	var err error

	shutdown := func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	// Set up propagator.
	// prop := newPropagator()
	// otel.SetTextMapPropagator(prop)

	// Set up trace provider.
	// tracerProvider, err := newTracerProvider()
	// if err != nil {
	// 	handleErr(err)
	// 	return shutdown, err
	// }
	// shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	// otel.SetTracerProvider(tracerProvider)

	res, err := newResource(serviceName)
	if err != nil {
		handleErr(err)
		return shutdown, err
	}

	loggerProvider, err := newLoggerProvider(ctx, res)
	if err != nil {
		handleErr(err)
		return shutdown, err
	}
	shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
	global.SetLoggerProvider(loggerProvider)

	meterProvider, err := newMeterProvider(ctx, res)
	if err != nil {
		handleErr(err)
		return shutdown, err
	}
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	return shutdown, err
}

// func newPropagator() propagation.TextMapPropagator {
// 	return propagation.NewCompositeTextMapPropagator(
// 		propagation.TraceContext{},
// 		propagation.Baggage{},
// 	)
// }

// func newTracerProvider() (*trace.TracerProvider, error) {
// 	traceExporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
// 	if err != nil {
// 		return nil, err
// 	}

// 	tracerProvider := trace.NewTracerProvider(
// 		trace.WithBatcher(traceExporter,
// 			// Default is 5s. Set to 1s for demonstrative purposes.
// 			trace.WithBatchTimeout(time.Second)),
// 	)
// 	return tracerProvider, nil
// }

func newResource(serviceName string) (*resource.Resource, error) {
	return resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
		semconv.ServiceVersion("0.1.0"),
	))
}

func newLoggerProvider(ctx context.Context, res *resource.Resource) (*log.LoggerProvider, error) {
	exporter, err := otlploggrpc.New(ctx)
	if err != nil {
		return nil, err
	}

	provider := log.NewLoggerProvider(
		log.WithResource(res),
		log.WithProcessor(log.NewBatchProcessor(exporter)),
	)

	return provider, nil
}

func newMeterProvider(ctx context.Context, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	exporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, err
	}

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter,
			// Default is 1m. Set to 3s for demonstrative purposes.
			sdkmetric.WithInterval(3*time.Second))),
	)

	err = runtime.Start(
		runtime.WithMeterProvider(provider),
	)
	if err != nil {
		return nil, err
	}

	host.Start(
		host.WithMeterProvider(provider),
	)
	if err != nil {
		return nil, err
	}

	otel.SetMeterProvider(provider)

	return provider, nil
}

func GetLogger(name string) *slog.Logger {
	return otelslog.NewLogger(name, otelslog.WithLoggerProvider(global.GetLoggerProvider()))
}

func GetMeter(name string) metric.Meter {
	return otel.GetMeterProvider().Meter(name)
}
