package connectprometheus

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bufbuild/connect-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Interceptor struct {
	clientRequests *prometheus.CounterVec
	clientDuration *prometheus.HistogramVec

	serverRequests *prometheus.CounterVec
	serverDuration *prometheus.HistogramVec
}

// NewInterceptor creates a new connect interceptor
// that registers metrics with the passed prometheus.Registerer.
func NewInterceptor(reg prometheus.Registerer) *Interceptor {
	labelNames := []string{"code", "method", "service", "type"}

	interceptor := &Interceptor{}

	interceptor.clientRequests = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "connect_client_requests_total",
		Help: "Tracks the number of connect client requests by code, method, service and type.",
	}, labelNames)

	interceptor.clientDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:                           "connect_client_requests_duration_seconds",
		Help:                           "Tracks the latencies of connect client requests by code, method, service and type.",
		NativeHistogramBucketFactor:    1.1,
		NativeHistogramMaxBucketNumber: 100,
	}, labelNames)

	interceptor.serverRequests = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "connect_server_requests_total",
		Help: "Tracks the number of connect server requests by code, method, service and type.",
	}, labelNames)

	interceptor.serverDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:                           "connect_server_requests_duration_seconds",
		Help:                           "Tracks the latencies of connect server requests by code, method, service and type.",
		NativeHistogramBucketFactor:    1.1,
		NativeHistogramMaxBucketNumber: 100,
	}, labelNames)

	return interceptor
}

func (i *Interceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		spec := req.Spec()
		procedure := strings.Split(spec.Procedure, "/")
		if len(procedure) != 3 {
			return nil, connect.NewError(
				connect.CodeInternal,
				fmt.Errorf("procedure in prometheus interceptor malformed: %s", spec.Procedure),
			)
		}
		service, method := procedure[1], procedure[2]

		start := time.Now()

		// Execute the actual request.
		resp, err := next(ctx, req)

		if spec.IsClient {
			i.clientRequests.WithLabelValues(
				code(err),
				method,
				service,
				streamType(spec.StreamType),
			).Inc()
			i.clientDuration.WithLabelValues(
				code(err),
				method,
				service,
				streamType(spec.StreamType),
			).Observe(time.Since(start).Seconds())
		} else {
			i.serverRequests.WithLabelValues(
				code(err),
				method,
				service,
				streamType(spec.StreamType),
			).Inc()
			i.serverDuration.WithLabelValues(
				code(err),
				method,
				service,
				streamType(spec.StreamType),
			).Observe(time.Since(start).Seconds())
		}

		return resp, err
	}
}

func (i *Interceptor) WrapStreamingClient(handle connect.StreamingClientFunc) connect.StreamingClientFunc {
	// nop for now
	return handle
}

func (i *Interceptor) WrapStreamingHandler(handle connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	// nop for now
	return handle
}

// code returns the code based on an error.
// If error is nil the code is ok.
func code(err error) string {
	if err == nil {
		return "ok"
	}
	return connect.CodeOf(err).String()
}

// streamType returns a string for the connect.StreamType.
func streamType(t connect.StreamType) string {
	switch t {
	case connect.StreamTypeUnary:
		return "unary"
	case connect.StreamTypeClient:
		return "client_stream"
	case connect.StreamTypeServer:
		return "server_stream"
	case connect.StreamTypeBidi:
		return "bidi_stream"
	default:
		return ""
	}
}
