// Package rocketmq adapts RocketMQ 5 ordinary messages. Constructors are inert;
// host transactions, durable failure audit and business receipts remain host-owned.
package rocketmq

import (
	"crypto/tls"
	"errors"
	"net"
	"time"

	"github.com/FangcunMount/reliable-messaging/transport"
)

type Config struct {
	// TLS defaults to system certificate verification, never the upstream insecure default.
	TLS                            *tls.Config
	Endpoint, AccessKey, SecretKey string
	Routes                         []transport.RouteBinding
	RequestTimeout                 time.Duration
	MaxInFlight                    int
}

func validate(c Config) (Config, error) {
	if _, _, err := net.SplitHostPort(c.Endpoint); err != nil {
		return c, errors.New("RocketMQ host:port endpoint required")
	}
	if c.AccessKey == "" || c.SecretKey == "" || c.RequestTimeout <= 0 || c.MaxInFlight < 1 || c.MaxInFlight > 1000 {
		return c, errors.New("credentials, bounded timeout and in-flight limit required")
	}
	if err := transport.ValidateRoutes(c.Routes); err != nil {
		return c, err
	}
	if c.TLS == nil {
		c.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		c.TLS = c.TLS.Clone()
	}
	c.Routes = append([]transport.RouteBinding(nil), c.Routes...)
	return c, nil
}
