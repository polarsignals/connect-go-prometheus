package connectprometheus

import (
	"context"
	"errors"
	"io"
	"testing"

	connect "connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectinprocess"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	unarySpec = connect.Spec{
		Procedure:  "/test.v1.TestService/Echo",
		StreamType: connect.StreamTypeUnary,
	}
	serverStreamSpec = connect.Spec{
		Procedure:  "/test.v1.TestService/Count",
		StreamType: connect.StreamTypeServer,
	}
)

func newTestClient(t *testing.T, reg prometheus.Registerer) *connect.Client {
	t.Helper()
	interceptor := NewInterceptor(reg)

	server := connect.NewServer(interceptor.ServerInterceptor())
	server.Register(
		connect.Method{Spec: unarySpec, Handler: func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
			req := &wrapperspb.StringValue{}
			if err := stream.Receive(req); err != nil {
				return err
			}
			if req.GetValue() == "fail" {
				return connect.NewError(connect.CodeInvalidArgument, "fail")
			}
			return stream.Send(req)
		}},
		connect.Method{Spec: serverStreamSpec, Handler: func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
			if err := stream.Receive(&wrapperspb.StringValue{}); err != nil {
				return err
			}
			for range 3 {
				if err := stream.Send(&wrapperspb.StringValue{Value: "tick"}); err != nil {
					return err
				}
			}
			return nil
		}},
	)

	return connect.NewClient(connectinprocess.New(server), interceptor.ClientInterceptor())
}

func TestInterceptorUnary(t *testing.T) {
	reg := prometheus.NewRegistry()
	client := newTestClient(t, reg)
	ctx := context.Background()

	require := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	require(client.CallUnary(ctx, unarySpec, &wrapperspb.StringValue{Value: "hi"}, &wrapperspb.StringValue{}))
	require(client.CallUnary(ctx, unarySpec, &wrapperspb.StringValue{Value: "hi"}, &wrapperspb.StringValue{}))
	err := client.CallUnary(ctx, unarySpec, &wrapperspb.StringValue{Value: "fail"}, &wrapperspb.StringValue{})
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected invalid_argument, got %v", err)
	}

	for _, tc := range []struct {
		metric string
		code   string
		want   float64
	}{
		{"connect_server_requests_total", "ok", 2},
		{"connect_server_requests_total", "invalid_argument", 1},
		{"connect_client_requests_total", "ok", 2},
		{"connect_client_requests_total", "invalid_argument", 1},
	} {
		got := counterValue(t, reg, tc.metric, map[string]string{
			"code": tc.code, "service": "test.v1.TestService", "method": "Echo", "type": "unary",
		})
		if got != tc.want {
			t.Errorf("%s{code=%q} = %v, want %v", tc.metric, tc.code, got, tc.want)
		}
	}
}

// A streaming caller may read to io.EOF and never call Close; the RPC must still be counted exactly once.
func TestInterceptorServerStreamWithoutClose(t *testing.T) {
	reg := prometheus.NewRegistry()
	client := newTestClient(t, reg)

	stream, err := client.CallServerStream(context.Background(), serverStreamSpec, &wrapperspb.StringValue{})
	if err != nil {
		t.Fatal(err)
	}
	received := 0
	for {
		err := stream.Receive(&wrapperspb.StringValue{})
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		received++
	}
	if received != 3 {
		t.Fatalf("received %d messages, want 3", received)
	}

	labels := map[string]string{"code": "ok", "service": "test.v1.TestService", "method": "Count", "type": "server_stream"}
	if got := counterValue(t, reg, "connect_client_requests_total", labels); got != 1 {
		t.Errorf("client counter = %v, want 1", got)
	}
	// Closing after io.EOF must not count the RPC a second time.
	_ = stream.Close()
	if got := counterValue(t, reg, "connect_client_requests_total", labels); got != 1 {
		t.Errorf("client counter after Close = %v, want 1", got)
	}
	if got := counterValue(t, reg, "connect_server_requests_total", labels); got != 1 {
		t.Errorf("server counter = %v, want 1", got)
	}
}

func counterValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	metrics:
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if labels[l.GetName()] != l.GetValue() {
					continue metrics
				}
			}
			return m.GetCounter().GetValue()
		}
	}
	return 0
}
