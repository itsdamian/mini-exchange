package marketdata

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ServerConfig configures the WebSocket server.
type ServerConfig struct {
	Interval     time.Duration // conflation: at most one frame per client per interval (default 100 ms)
	WriteTimeout time.Duration // a client that does not accept a frame in this time is disconnected (default 1 s)
	MaxClients   int           // new connections beyond this get 503 (default 5000)
	Registerer   prometheus.Registerer
}

// Server serves a Feed to WebSocket clients at /ws.
type Server struct {
	feed    *Feed
	cfg     ServerConfig
	clients atomic.Int64

	sent     prometheus.Counter
	slow     prometheus.Counter
	refused  prometheus.Counter
	accepted prometheus.Counter
}

// NewServer returns a Server for the feed.
func NewServer(feed *Feed, cfg ServerConfig) *Server {
	if cfg.Interval <= 0 {
		cfg.Interval = 100 * time.Millisecond
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = time.Second
	}
	if cfg.MaxClients <= 0 {
		cfg.MaxClients = 5000
	}
	reg := promauto.With(cfg.Registerer)
	s := &Server{feed: feed, cfg: cfg,
		sent:     reg.NewCounter(prometheus.CounterOpts{Name: "mx_md_messages_sent_total", Help: "Frames written to clients."}),
		slow:     reg.NewCounter(prometheus.CounterOpts{Name: "mx_md_slow_disconnects_total", Help: "Clients disconnected because they did not accept a frame in time."}),
		refused:  reg.NewCounter(prometheus.CounterOpts{Name: "mx_md_clients_refused_total", Help: "Connections refused at the client limit."}),
		accepted: reg.NewCounter(prometheus.CounterOpts{Name: "mx_md_clients_accepted_total", Help: "Connections accepted."}),
	}
	promauto.With(cfg.Registerer).NewGaugeFunc(prometheus.GaugeOpts{Name: "mx_md_clients", Help: "Connected clients."},
		func() float64 { return float64(s.clients.Load()) })
	return s
}

// Clients returns the number of connected clients.
func (s *Server) Clients() int { return int(s.clients.Load()) }

// Handler serves GET /ws.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws", s.serve)
	return mux
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	if s.clients.Add(1) > int64(s.cfg.MaxClients) {
		s.clients.Add(-1)
		s.refused.Inc()
		http.Error(w, "too many clients", http.StatusServiceUnavailable)
		return
	}
	defer s.clients.Add(-1)

	// Origin is not checked: the feed is public, read-only market data.
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	s.accepted.Inc()
	defer func() { _ = c.CloseNow() }()

	// Clients only listen. CloseRead discards anything they send and returns
	// a context that ends when the connection does.
	ctx := c.CloseRead(r.Context())

	var sentVersion uint64
	send := func() bool {
		fr := s.feed.Frame()
		if fr.Version == sentVersion {
			return true
		}
		wctx, cancel := context.WithTimeout(ctx, s.cfg.WriteTimeout)
		err := c.Write(wctx, websocket.MessageText, fr.Data)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				s.slow.Inc()
				c.Close(websocket.StatusPolicyViolation, "too slow")
			}
			return false
		}
		sentVersion = fr.Version
		s.sent.Inc()
		return true
	}
	if !send() { // the current state right away, then conflated updates
		return
	}
	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !send() {
				return
			}
		}
	}
}
