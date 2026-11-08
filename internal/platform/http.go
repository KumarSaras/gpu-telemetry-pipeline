package platform

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

func HTTP(addr string, handler http.Handler, writeTimeout time.Duration, log *slog.Logger) (*http.Server, <-chan error, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	srv := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: writeTimeout, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		done <- err
	}()
	log.Info("HTTP listening", "address", ln.Addr().String())
	return srv, done, nil
}
func ShutdownHTTP(srv *http.Server, d time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		_ = srv.Close()
	}
}
