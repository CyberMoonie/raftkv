// Command raftkv-lab runs a raftkv cluster on a simulated network inside one
// process and serves the Raft Lab, a dashboard to watch it and break it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/CyberMoonie/raftkv/internal/lab"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "address of the dashboard")
	redisBind := flag.String("redis-bind", "127.0.0.1", "host the nodes' Redis ports listen on")
	redisPort := flag.Int("redis-port", 7379, "Redis port of node n1; n2 gets the next one, and so on (0 disables)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	l, err := lab.New(lab.Options{RedisBind: *redisBind, RedisPort: *redisPort, Logger: logger})
	if err != nil {
		logger.Error("starting the lab", "err", err)
		os.Exit(1)
	}
	defer l.Close()

	srv := &http.Server{Addr: *addr, Handler: l.Handler()}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		srv.Shutdown(shutdown)
	}()

	host, port, _ := net.SplitHostPort(*addr)
	if host == "" || host == "0.0.0.0" {
		host = "localhost"
	}
	fmt.Printf("Raft Lab: http://%s\n", net.JoinHostPort(host, port))
	if *redisPort > 0 {
		fmt.Printf("Redis ports: %d-%d (try redis-cli -c -p %d)\n", *redisPort, *redisPort+4, *redisPort)
	}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("serving the dashboard", "err", err)
		os.Exit(1)
	}
}
