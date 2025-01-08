package pool

import (
	"net"
	"time"
)

type ConnFactory struct {
	dialTimeout time.Duration
	address     string
}

func NewConnFactory(address string, dialTimeout time.Duration) *ConnFactory {
	return &ConnFactory{
		address:     address,
		dialTimeout: dialTimeout,
	}
}

func (f *ConnFactory) CreateConnection() (net.Conn, error) {
	return net.DialTimeout("tcp", f.address, f.dialTimeout)
}
