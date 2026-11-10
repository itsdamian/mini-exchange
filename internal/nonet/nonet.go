// Package nonet keeps tests off the real network. Install makes the default
// HTTP transport (and so every client that uses it, including WebSocket
// dials) refuse any host that is not loopback, so a test that tries to reach
// an external service fails at once with ErrExternal instead of silently
// depending on the internet.
package nonet

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

// ErrExternal is returned for a dial to a non-loopback host.
var ErrExternal = errors.New("nonet: tests must not reach external hosts")

// Dial dials loopback addresses only.
func Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if host != "localhost" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return nil, ErrExternal
		}
	}
	d := net.Dialer{Timeout: 5 * time.Second}
	return d.DialContext(ctx, network, addr)
}

// Install replaces the default transport's dialer. Call it from TestMain.
func Install() {
	if t, ok := http.DefaultTransport.(*http.Transport); ok {
		t.DialContext = Dial
		t.Proxy = nil
	}
}
