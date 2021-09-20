package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"syscall"

	"github.com/go-kit/kit/log"
	consulsd "github.com/go-kit/kit/sd/consul"
	kitgrpc "github.com/go-kit/kit/transport/grpc"
	"github.com/google/uuid"
	"github.com/hashicorp/consul/api"
	"github.com/maolonglong/microservices-example/pb"
	"github.com/maolonglong/microservices-example/pkg/addendpoint"
	"github.com/maolonglong/microservices-example/pkg/addservice"
	"github.com/maolonglong/microservices-example/pkg/addtransport"
	"github.com/oklog/run"
	"github.com/spf13/cast"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var (
	httpPort = flag.Int("http_port", 8081, "HTTP listen address")
	grpcPort = flag.Int("grpc_port", 9091, "gRPC listen address")
)

func main() {
	flag.Parse()

	var logger log.Logger
	{
		logger = log.NewLogfmtLogger(os.Stderr)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC)
		logger = log.With(logger, "caller", log.DefaultCaller)
	}

	var (
		service     = addservice.New(logger)
		endpoints   = addendpoint.New(service, logger)
		httpHandler = addtransport.NewHTTPHandler(endpoints, logger)
		grpcServer  = addtransport.NewGRPCServer(endpoints, logger)
	)

	var g run.Group
	{
		httpAddr := ":" + cast.ToString(*httpPort)
		httpListener, err := net.Listen("tcp", httpAddr)
		if err != nil {
			logger.Log("transport", "HTTP", "during", "Listen", "err", err)
			os.Exit(1)
		}
		g.Add(func() error {
			logger.Log("transport", "HTTP", "addr", httpAddr)
			return http.Serve(httpListener, httpHandler)
		}, func(error) {
			httpListener.Close()
		})
	}
	{
		grpcAddr := ":" + cast.ToString(*grpcPort)
		grpcListener, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			logger.Log("transport", "gRPC", "during", "Listen", "err", err)
			os.Exit(1)
		}
		g.Add(func() error {
			logger.Log("transport", "gRPC", "addr", grpcAddr)
			baseServer := grpc.NewServer(grpc.UnaryInterceptor(kitgrpc.Interceptor))
			pb.RegisterAddServiceServer(baseServer, grpcServer)

			healthServer := health.NewServer()
			healthServer.SetServingStatus("addsvc", grpc_health_v1.HealthCheckResponse_SERVING)
			grpc_health_v1.RegisterHealthServer(baseServer, healthServer)

			return baseServer.Serve(grpcListener)
		}, func(error) {
			grpcListener.Close()
		})
	}

	g.Add(run.SignalHandler(context.Background(), syscall.SIGINT, syscall.SIGTERM))

	var client consulsd.Client
	{
		consulClient, err := api.NewClient(api.DefaultConfig())
		if err != nil {
			logger.Log("err", err)
			os.Exit(1)
		}
		client = consulsd.NewClient(consulClient)
	}

	registrar := consulsd.NewRegistrar(
		client,
		&api.AgentServiceRegistration{
			ID:   uuid.NewString(),
			Name: "addsvc",
			// Port:    *httpPort,
			Port:    *grpcPort,
			Address: "localhost",
			Checks: api.AgentServiceChecks{
				{
					Interval: "5s",
					Timeout:  "2s",
					HTTP:     fmt.Sprintf("http://localhost:%d/health", *httpPort),
				},
				{
					Interval: "5s",
					Timeout:  "2s",
					GRPC:     fmt.Sprintf("localhost:%d/addsvc", *grpcPort),
				},
			},
		},
		logger,
	)

	registrar.Register()
	defer registrar.Deregister()

	logger.Log("exit", g.Run())
}
