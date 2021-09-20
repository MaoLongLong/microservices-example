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
	"github.com/google/uuid"
	"github.com/hashicorp/consul/api"
	"github.com/microservices-example/addsvc/addendpoint"
	"github.com/microservices-example/addsvc/addservice"
	"github.com/microservices-example/addsvc/addtransport"
	"github.com/oklog/run"
	"github.com/spf13/cast"
)

var (
	httpPort = flag.Int("http_port", 8081, "HTTP listen address")
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
			ID:      uuid.NewString(),
			Name:    "addsvc",
			Port:    *httpPort,
			Address: "localhost",
			Check: &api.AgentServiceCheck{
				Interval: "5s",
				HTTP:     fmt.Sprintf("http://localhost:%d/health", *httpPort),
			},
		},
		logger,
	)

	registrar.Register()
	defer registrar.Deregister()

	logger.Log("exit", g.Run())
}
