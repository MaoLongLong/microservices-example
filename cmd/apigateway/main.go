package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/sd"
	consulsd "github.com/go-kit/kit/sd/consul"
	"github.com/go-kit/kit/sd/lb"
	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/gorilla/mux"
	"github.com/hashicorp/consul/api"
	"github.com/oklog/run"
	"github.com/spf13/cast"
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

	ctx := context.Background()
	r := mux.NewRouter()

	var (
		tags        = []string{}
		passingOnly = true
		sum         endpoint.Endpoint
		concat      endpoint.Endpoint
		instancer   = consulsd.NewInstancer(client, logger, "addsvc", tags, passingOnly)
	)
	{
		factory := addsvcFactory(ctx, "GET", "/sum")
		endpointer := sd.NewEndpointer(instancer, factory, logger)
		balancer := lb.NewRoundRobin(endpointer)
		retry := lb.Retry(3, 500*time.Millisecond, balancer)
		sum = retry
	}
	{
		factory := addsvcFactory(ctx, "GET", "/concat")
		endpointer := sd.NewEndpointer(instancer, factory, logger)
		balancer := lb.NewRoundRobin(endpointer)
		retry := lb.Retry(3, 500*time.Millisecond, balancer)
		concat = retry
	}

	r.Handle("/addsvc/sum", httptransport.NewServer(sum, decodeSumRequest, encodeJSONResponse))
	r.Handle("/addsvc/concat", httptransport.NewServer(concat, decodeConcatRequest, encodeJSONResponse))

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

func addsvcFactory(ctx context.Context, method, path string) sd.Factory {
	return func(instance string) (endpoint.Endpoint, io.Closer, error) {
		if !strings.HasPrefix(instance, "http") {
			instance = "http://" + instance
		}
		tgt, err := url.Parse(instance)
		if err != nil {
			return nil, nil, err
		}
		tgt.Path = path

		var (
			enc httptransport.EncodeRequestFunc
			dec httptransport.DecodeResponseFunc
		)
		switch path {
		case "/sum":
			enc, dec = encodeJSONRequest, decodeSumResponse
		case "/concat":
			enc, dec = encodeJSONRequest, decodeConcatResponse
		default:
			return nil, nil, fmt.Errorf("unknown addsvc path %q", path)
		}

		return httptransport.NewClient(method, tgt, enc, dec).Endpoint(), nil, nil
	}
}

func encodeJSONRequest(_ context.Context, req *http.Request, request interface{}) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(request); err != nil {
		return err
	}
	req.Body = ioutil.NopCloser(&buf)
	return nil
}

func encodeJSONResponse(_ context.Context, w http.ResponseWriter, response interface{}) error {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	return json.NewEncoder(w).Encode(response)
}

func decodeSumResponse(ctx context.Context, r *http.Response) (interface{}, error) {
	var response struct {
		V   int    `json:"v,omitempty"`
		Err string `json:"error,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&response); err != nil {
		return nil, err
	}
	return response, nil
}

func decodeConcatResponse(ctx context.Context, r *http.Response) (interface{}, error) {
	var response struct {
		V   string `json:"v,omitempty"`
		Err string `json:"error,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&response); err != nil {
		return nil, err
	}
	return response, nil
}

func decodeSumRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var request struct {
		A int `json:"a"`
		B int `json:"b"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

func decodeConcatRequest(ctx context.Context, r *http.Request) (interface{}, error) {
	var request struct {
		A string `json:"a"`
		B string `json:"b"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}
