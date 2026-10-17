package connectprometheus

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"

	connect "connectrpc.com/connect/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Interceptor holds the request counters shared by its server and client interceptors,
// so one Interceptor can instrument both sides without registering the metrics twice.
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

// ServerInterceptor returns the interceptor to pass to connect.NewServer.
func (i *Interceptor) ServerInterceptor() connect.ServerInterceptor {
	return func(next connect.ServerFunc) connect.ServerFunc {
		return func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) error {
			service, method, err := splitProcedure(spec.Procedure)
			if err != nil {
				return err
			}

			// The handler has finished the whole RPC once next returns, streaming or not.
			err = next(ctx, spec, stream)

			i.serverRequests.WithLabelValues(
				code(err),
				method,
				service,
				streamType(spec.StreamType),
			).Inc()

			return err
		}
	}
}

// ClientInterceptor returns the interceptor to pass to connect.NewClient.
func (i *Interceptor) ClientInterceptor() connect.ClientInterceptor {
	return func(next connect.ClientFunc) connect.ClientFunc {
		return func(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
			service, method, err := splitProcedure(spec.Procedure)
			if err != nil {
				return nil, err
			}

			counter := i.clientRequests.MustCurryWith(prometheus.Labels{
				"method":  method,
				"service": service,
				"type":    streamType(spec.StreamType),
			})

			stream, err := next(ctx, spec)
			if err != nil {
				counter.WithLabelValues(code(err)).Inc()
				return nil, err
			}
			return &clientStream{ClientStream: stream, counter: counter}, nil
		}
	}
}

// clientStream counts the RPC when it ends. A v2 client interceptor only wraps
// opening the stream, so the outcome is only known later: either Receive reports
// io.EOF (streaming callers may stop there without calling Close) or Close is
// called, which Client.CallUnary always does.
type clientStream struct {
	connect.ClientStream
	counter *prometheus.CounterVec

	mu   sync.Mutex
	err  error
	once sync.Once
}

func (s *clientStream) Send(msg any) error {
	err := s.ClientStream.Send(msg)
	// Send reports io.EOF when the server ended the RPC; Receive carries the actual status.
	if !errors.Is(err, io.EOF) {
		s.setErr(err)
	}
	return err
}

func (s *clientStream) CloseSend() error {
	err := s.ClientStream.CloseSend()
	s.setErr(err)
	return err
}

func (s *clientStream) Receive(msg any) error {
	err := s.ClientStream.Receive(msg)
	if errors.Is(err, io.EOF) {
		s.observe()
		return err
	}
	s.setErr(err)
	return err
}

func (s *clientStream) Close() error {
	err := s.ClientStream.Close()
	s.observe()
	return err
}

func (s *clientStream) setErr(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
}

func (s *clientStream) observe() {
	s.once.Do(func() {
		s.mu.Lock()
		err := s.err
		s.mu.Unlock()
		s.counter.WithLabelValues(code(err)).Inc()
	})
}

func splitProcedure(procedure string) (service, method string, err error) {
	parts := strings.Split(procedure, "/")
	if len(parts) != 3 {
		return "", "", connect.Errorf(
			connect.CodeInternal,
			"procedure in prometheus interceptor malformed: %s", procedure,
		)
	}
	return parts[1], parts[2], nil
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
