package ports

import (
	"context"
	"net"
	"time"
)

type Pool interface {
	GetReadConn(ctx context.Context) (net.Conn, error)
	GetWriteConn(ctx context.Context) (net.Conn, error)
	ReturnConn(conn net.Conn, pool string)
	Close() error // error dönüş tipini ekledik
	Stats() (active, idle int, avgWaitTime time.Duration)
}
