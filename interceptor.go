package connectprometheus

import (
	"context"
	"fmt"
	"strings"

	"github.com/bufbuild/connect-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Interceptor struct {
	clientRequests *prometheus.CounterVec
	serverRequests *prometheus.CounterVec
}

// NewInterceptor creates a new connect interceptor
// that registers metrics with the passed prometheus.Registerer.
func NewInterceptor(reg prometheus.Registerer) *Interceptor {
	labelCode := "code"
	labelMethod := "method"
	labelService := "service"
	labelType := "type"

	interceptor := &Interceptor{}

	interceptor.clientRequests = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "connect_client_requests_total",
		Help: "Tracks the number of connect client requests by code, method, service and type.",
	}, []string{labelCode, labelMethod, labelService, labelType})

	interceptor.serverRequests = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "connect_server_requests_total",
		Help: "Tracks the number of connect server requests by code, method, service and type.",
	}, []string{labelCode, labelMethod, labelService, labelType})

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

		// Execute the actual request.
		resp, err := next(ctx, req)

		if spec.IsClient {
			i.clientRequests.WithLabelValues(
				code(err),
				method,
				service,
				streamType(spec.StreamType),
			).Inc()
		} else {
			i.serverRequests.WithLabelValues(
				code(err),
				method,
				service,
				streamType(spec.StreamType),
			).Inc()
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
