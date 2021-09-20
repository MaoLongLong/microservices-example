package main

import (
	"context"
	"flag"
	"io"
	"net"
	"net/http"
	"os"
	"syscall"
	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/sd"
	consulsd "github.com/go-kit/kit/sd/consul"
	"github.com/go-kit/kit/sd/lb"
	"github.com/gorilla/mux"
	"github.com/hashicorp/consul/api"
	"github.com/maolonglong/microservices-example/pkg/addendpoint"
	"github.com/maolonglong/microservices-example/pkg/addservice"
	"github.com/maolonglong/microservices-example/pkg/addtransport"
	"github.com/oklog/run"
	"github.com/spf13/cast"
	"google.golang.org/grpc"
)

var (
	httpPort = flag.Int("http_port", 8080, "Address for HTTP (JSON) server")
)

func main() {
	flag.Parse()

	var logger log.Logger
	{
		logger = log.NewLogfmtLogger(os.Stderr)
		logger = log.With(logger, "ts", log.DefaultTimestampUTC)
		logger = log.With(logger, "caller", log.DefaultCaller)
	}

	var client consulsd.Client
	{
		consulClient, err := api.NewClient(api.DefaultConfig())
		if err != nil {
			logger.Log("err", err)
			os.Exit(1)
		}
		client = consulsd.NewClient(consulClient)
	}

	r := mux.NewRouter()

	var (
		tags        = []string{}
		passingOnly = true
		endpoints   = addendpoint.Set{}
		instancer   = consulsd.NewInstancer(client, logger, "addsvc", tags, passingOnly)
	)
	{
		factory := addsvcFactory(addendpoint.MakeSumEndpoint, logger)
		endpointer := sd.NewEndpointer(instancer, factory, logger)
		balancer := lb.NewRoundRobin(endpointer)
		retry := lb.Retry(3, 500*time.Millisecond, balancer)
		endpoints.SumEndpoint = retry
	}
	{
		factory := addsvcFactory(addendpoint.MakeConcatEndpoint, logger)
		endpointer := sd.NewEndpointer(instancer, factory, logger)
		balancer := lb.NewRoundRobin(endpointer)
		retry := lb.Retry(3, 500*time.Millisecond, balancer)
		endpoints.ConcatEndpoint = retry
	}

	r.PathPrefix("/addsvc").Handler(http.StripPrefix("/addsvc", addtransport.NewHTTPHandler(endpoints, logger)))

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
			return http.Serve(httpListener, r)
		}, func(error) {
			httpListener.Close()
		})
	}
	g.Add(run.SignalHandler(context.Background(), syscall.SIGINT, syscall.SIGTERM))
	logger.Log("exit", g.Run())
}

func addsvcFactory(makeEndpoint func(addservice.Service) endpoint.Endpoint, logger log.Logger) sd.Factory {
	return func(instance string) (endpoint.Endpoint, io.Closer, error) {
		conn, err := grpc.Dial(instance, grpc.WithInsecure())
		if err != nil {
			return nil, nil, err
		}
		service := addtransport.NewGRPCClient(conn, logger)
		endpoint := makeEndpoint(service)
		return endpoint, conn, nil
	}
}
