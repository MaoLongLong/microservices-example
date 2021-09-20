package addservice

import (
	"context"

	"github.com/go-kit/kit/log"
)

type Middleware func(next Service) Service

func LoggingMiddleware(logger log.Logger) Middleware {
	return func(next Service) Service {
		return loggingMiddleware{logger, next}
	}
}

type loggingMiddleware struct {
	logger log.Logger
	next   Service
}

func (mw loggingMiddleware) Sum(ctx context.Context, a, b int) (v int, err error) {
	defer func() {
		mw.logger.Log(
			"method", "Sum",
			"a", a,
			"b", b,
			"v", v,
			"err", err,
		)
	}()
	return mw.next.Sum(ctx, a, b)
}

func (mw loggingMiddleware) Concat(ctx context.Context, a, b string) (v string, err error) {
	defer func() {
		mw.logger.Log("method", "Concat",
			"a", a,
			"b", b,
			"v", v,
			"err", err,
		)
	}()
	return mw.next.Concat(ctx, a, b)
}
