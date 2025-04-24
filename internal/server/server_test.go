package server

import (
	"context"
	"flag"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	api "github.com/cipherzzz/commitlog/api/v1"
	"github.com/cipherzzz/commitlog/internal/auth"
	"github.com/cipherzzz/commitlog/internal/config"
	"github.com/cipherzzz/commitlog/internal/log"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var debug = flag.Bool("debug", false, "Enable observability")

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, rootClient api.LogClient, nobodyClient api.LogClient, config *Config){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"unauthorized fails":                                 testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (rootClient api.LogClient, nobodyClient api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// Create a temporary log file
	logFile, err := ioutil.TempFile("", "server-test-*.log")
	require.NoError(t, err)
	t.Logf("log file: %s", logFile.Name())

	newClient := func(crtPath, keypath string, insecureClient, differentCA bool) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {

		// Test if fails with an insecure client
		var conn *grpc.ClientConn
		var opts []grpc.DialOption
		if insecureClient {
			opts = []grpc.DialOption{grpc.WithInsecure()}
			conn, err = grpc.Dial(l.Addr().String(), opts...)
			require.NoError(t, err)
		} else {
			clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
				CertFile: crtPath,
				KeyFile:  keypath,
				CAFile:   config.CAFile})
			require.NoError(t, err)

			clientCreds := credentials.NewTLS(clientTLSConfig)

			opts = []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}
			conn, err = grpc.Dial(l.Addr().String(), opts...)
			require.NoError(t, err)
		}
		client := api.NewLogClient(conn)
		return conn, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(config.RootClientCertFile, config.RootClientKeyFile, false, false)

	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile, false, false)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)

	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := ioutil.TempFile("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		tracesLogFile, err := ioutil.TempFile("", "traces-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
			ReportingInterval: time.Second,
		})
		require.NoError(t, err)
		err = telemetryExporter.Start()
		require.NoError(t, err)
	}

	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
		LogFile:    logFile.Name(),
	}
	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
		clog.Remove()
		os.Remove(logFile.Name())
	}

}

func testProduceConsume(t *testing.T, client api.LogClient, _ api.LogClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{Value: []byte("hello world")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)

	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, _ api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err %v, want %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, _ api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}

	produceStream, err := client.ProduceStream(ctx)
	require.NoError(t, err)

	for offset, record := range records {
		err := produceStream.Send(&api.ProduceRequest{Record: record})
		require.NoError(t, err)
		res, err := produceStream.Recv()
		require.NoError(t, err)
		if res.Offset != uint64(offset) {
			t.Fatalf("got offset %d, want %d", res.Offset, offset)
		}
	}

	consumeStream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	require.NoError(t, err)

	for i, record := range records {
		res, err := consumeStream.Recv()
		require.NoError(t, err)
		require.Equal(t, res.Record, &api.Record{
			Value:  record.Value,
			Offset: uint64(i),
		})
	}
}

func testUnauthorized(t *testing.T, _ api.LogClient, nobodyClient api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := nobodyClient.Produce(ctx, &api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}})
	if produce != nil {
		t.Fatal("produce not nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code %v, want %v", gotCode, wantCode)
	}

	consume, err := nobodyClient.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code %v, want %v", gotCode, wantCode)
	}
}
