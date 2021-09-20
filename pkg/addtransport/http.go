package addtransport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-kit/kit/circuitbreaker"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/ratelimit"
	"github.com/go-kit/kit/transport"
	httptransport "github.com/go-kit/kit/transport/http"
	"github.com/gorilla/mux"
	"github.com/microservices-example/addsvc/addendpoint"
	"github.com/microservices-example/addsvc/addservice"
	"github.com/sony/gobreaker"
	"golang.org/x/time/rate"
)

func NewHTTPHandler(endpoints addendpoint.Set, logger log.Logger) http.Handler {
	options := []httptransport.ServerOption{
		httptransport.ServerErrorEncoder(errorEncoder),
		httptransport.ServerErrorHandler(transport.NewLogErrorHandler(logger)),
	}

	r := mux.NewRouter()
	r.Methods(http.MethodGet).Path("/sum").Handler(httptransport.NewServer(
		endpoints.SumEndpoint,
		decodeHTTPSumRequest,
		encodeHTTPGenericResponse,
		options...,
	))

	r.Methods(http.MethodGet).Path("/concat").Handler(httptransport.NewServer(
		endpoints.ConcatEndpoint,
		decodeHTTPConcatRequest,
		encodeHTTPGenericResponse,
		options...,
	))

	r.Methods(http.MethodGet).Path("/health").HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf8")
		w.Write([]byte(`{"status":"ok"}`))
	})

	return r
}

func NewHTTPClient(instance string, logger log.Logger) (addservice.Service, error) {
	if !strings.HasPrefix(instance, "http") {
		instance = "http://" + instance
	}
	u, err := url.Parse(instance)
	if err != nil {
		return nil, err
	}

	limiter := ratelimit.NewErroringLimiter(rate.NewLimiter(rate.Every(time.Second), 100))

	var sumEndpoint endpoint.Endpoint
	{
		sumEndpoint = httptransport.NewClient(
			http.MethodGet,
			copyURL(u, "/sum"),
			encodeHTTPGenericRequest,
			decodeHTTPSumResponse,
		).Endpoint()
		sumEndpoint = limiter(sumEndpoint)
		sumEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    "Sum",
			Timeout: 30 * time.Second,
		}))(sumEndpoint)
	}

	var concatEndpoint endpoint.Endpoint
	{
		concatEndpoint = httptransport.NewClient(
			http.MethodGet,
			copyURL(u, "/concat"),
			encodeHTTPGenericRequest,
			decodeHTTPConcatResponse,
		).Endpoint()
		concatEndpoint = limiter(concatEndpoint)
		concatEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    "Concat",
			Timeout: 10 * time.Second,
		}))(concatEndpoint)
	}

	return addendpoint.Set{
		SumEndpoint:    sumEndpoint,
		ConcatEndpoint: concatEndpoint,
	}, nil
}

func copyURL(base *url.URL, path string) *url.URL {
	next := *base
	next.Path = path
	return &next
}

func errorEncoder(_ context.Context, err error, w http.ResponseWriter) {
	w.WriteHeader(err2code(err))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(errorWrapper{Error: err.Error()})
}

func err2code(err error) int {
	switch err {
	case addservice.ErrTwoZeroes, addservice.ErrMaxSizeExceeded, addservice.ErrIntOverflow:
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

type errorWrapper struct {
	Error string `json:"error"`
}

func decodeHTTPSumRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req addendpoint.SumRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	return req, err
}

func decodeHTTPConcatRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var req addendpoint.ConcatRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	return req, err
}

func decodeHTTPSumResponse(_ context.Context, r *http.Response) (interface{}, error) {
	if r.StatusCode != http.StatusOK {
		return nil, errors.New(r.Status)
	}
	var resp addendpoint.SumResponse
	err := json.NewDecoder(r.Body).Decode(&resp)
	return resp, err
}

func decodeHTTPConcatResponse(_ context.Context, r *http.Response) (interface{}, error) {
	if r.StatusCode != http.StatusOK {
		return nil, errors.New(r.Status)
	}
	var resp addendpoint.ConcatResponse
	err := json.NewDecoder(r.Body).Decode(&resp)
	return resp, err
}

func encodeHTTPGenericResponse(ctx context.Context, w http.ResponseWriter, response interface{}) error {
	if f, ok := response.(endpoint.Failer); ok && f.Failed() != nil {
		errorEncoder(ctx, f.Failed(), w)
		return nil
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	return json.NewEncoder(w).Encode(response)
}

func encodeHTTPGenericRequest(_ context.Context, r *http.Request, request interface{}) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(request); err != nil {
		return err
	}
	r.Body = io.NopCloser(&buf)
	return nil
}
