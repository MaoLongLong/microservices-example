package addtransport

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/kit/circuitbreaker"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/ratelimit"
	"github.com/go-kit/kit/transport"
	grpctransport "github.com/go-kit/kit/transport/grpc"
	"github.com/maolonglong/microservices-example/pb"
	"github.com/maolonglong/microservices-example/pkg/addendpoint"
	"github.com/maolonglong/microservices-example/pkg/addservice"
	"github.com/sony/gobreaker"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

type grpcServer struct {
	sum    grpctransport.Handler
	concat grpctransport.Handler
}

func NewGRPCServer(endpoints addendpoint.Set, logger log.Logger) pb.AddServiceServer {
	options := []grpctransport.ServerOption{
		grpctransport.ServerErrorHandler(transport.NewLogErrorHandler(logger)),
	}

	return &grpcServer{
		sum: grpctransport.NewServer(
			endpoints.SumEndpoint,
			decodeGRPCSumRequest,
			encodeGRPCSumResponse,
			options...,
		),
		concat: grpctransport.NewServer(
			endpoints.ConcatEndpoint,
			decodeGRPCConcatRequest,
			encodeGRPCConcatResponse,
			options...,
		),
	}
}

func (s *grpcServer) Sum(ctx context.Context, req *pb.SumRequest) (*pb.SumResponse, error) {
	_, resp, err := s.sum.ServeGRPC(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.SumResponse), nil
}

func (s *grpcServer) Concat(ctx context.Context, req *pb.ConcatRequest) (*pb.ConcatResponse, error) {
	_, resp, err := s.concat.ServeGRPC(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.(*pb.ConcatResponse), nil
}

func NewGRPCClient(conn *grpc.ClientConn, logger log.Logger) addservice.Service {
	limiter := ratelimit.NewErroringLimiter(rate.NewLimiter(rate.Every(time.Second), 100))

	var sumEndpoint endpoint.Endpoint
	{
		sumEndpoint = grpctransport.NewClient(
			conn,
			"pb.AddService",
			"Sum",
			encodeGRPCSumRequest,
			decodeGRPCSumResponse,
			pb.SumResponse{},
		).Endpoint()
		sumEndpoint = limiter(sumEndpoint)
		sumEndpoint = circuitbreaker.Gobreaker(gobreaker.NewCircuitBreaker(gobreaker.Settings{
			Name:    "Sum",
			Timeout: 30 * time.Second,
		}))(sumEndpoint)
	}

	var concatEndpoint endpoint.Endpoint
	{
		concatEndpoint = grpctransport.NewClient(
			conn,
			"pb.AddService",
			"Concat",
			encodeGRPCConcatRequest,
			decodeGRPCConcatResponse,
			pb.ConcatResponse{},
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
	}
}

func decodeGRPCSumRequest(_ context.Context, request interface{}) (interface{}, error) {
	req := request.(*pb.SumRequest)
	return addendpoint.SumRequest{A: int(req.A), B: int(req.B)}, nil
}

func decodeGRPCConcatRequest(_ context.Context, request interface{}) (interface{}, error) {
	req := request.(*pb.ConcatRequest)
	return addendpoint.ConcatRequest{A: req.A, B: req.B}, nil
}

func decodeGRPCSumResponse(_ context.Context, response interface{}) (interface{}, error) {
	resp := response.(*pb.SumResponse)
	return addendpoint.SumResponse{V: int(resp.V), Err: str2err(resp.Err)}, nil
}

func decodeGRPCConcatResponse(_ context.Context, response interface{}) (interface{}, error) {
	resp := response.(*pb.ConcatResponse)
	return addendpoint.ConcatResponse{V: resp.V, Err: str2err(resp.Err)}, nil
}

func encodeGRPCSumResponse(_ context.Context, response interface{}) (interface{}, error) {
	resp := response.(addendpoint.SumResponse)
	return &pb.SumResponse{V: int64(resp.V), Err: err2str(resp.Err)}, nil
}

func encodeGRPCConcatResponse(_ context.Context, response interface{}) (interface{}, error) {
	resp := response.(addendpoint.ConcatResponse)
	return &pb.ConcatResponse{V: resp.V, Err: err2str(resp.Err)}, nil
}

func encodeGRPCSumRequest(_ context.Context, request interface{}) (interface{}, error) {
	req := request.(addendpoint.SumRequest)
	return &pb.SumRequest{A: int64(req.A), B: int64(req.B)}, nil
}

func encodeGRPCConcatRequest(_ context.Context, request interface{}) (interface{}, error) {
	req := request.(addendpoint.ConcatRequest)
	return &pb.ConcatRequest{A: req.A, B: req.B}, nil
}

func str2err(s string) error {
	if s == "" {
		return nil
	}
	return errors.New(s)
}

func err2str(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
