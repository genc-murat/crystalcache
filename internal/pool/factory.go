package pool

import (
	"net"
	"time"
)

type ConnFactory struct {
	address     string
	dialTimeout time.Duration
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
