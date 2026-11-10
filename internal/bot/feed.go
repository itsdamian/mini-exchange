// Package bot generates synthetic order flow: a market maker quoting around a
// reference price plus random taker orders (FR-8). The reference price comes
// from a recorded file (default, deterministic and offline) or from the live
// Coinbase WebSocket feed.
package bot

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/coder/websocket"
)

// PriceSource yields reference prices. Next returns io.EOF when a recorded
// source is exhausted.
type PriceSource interface {
	Next(ctx context.Context) (float64, error)
	Close() error
}

// FileSource replays prices recorded in a file.
type FileSource struct {
	prices []float64
	i      int
}

// LoadFile reads prices from a CSV file (a "price" column named in the header,
// or else the last column) or from a JSON array of numbers or of objects with
// a "price" field. The file extension selects the parser.
func LoadFile(path string) (*FileSource, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var prices []float64
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		prices, err = parseJSON(f)
	default:
		prices, err = parseCSV(f)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(prices) == 0 {
		return nil, fmt.Errorf("%s: no prices", path)
	}
	return &FileSource{prices: prices}, nil
}

// NewFileSource returns a source over fixed prices (tests).
func NewFileSource(prices []float64) *FileSource { return &FileSource{prices: prices} }

// Len is the number of recorded prices.
func (s *FileSource) Len() int { return len(s.prices) }

// Next returns the next recorded price.
func (s *FileSource) Next(context.Context) (float64, error) {
	if s.i >= len(s.prices) {
		return 0, io.EOF
	}
	p := s.prices[s.i]
	s.i++
	return p, nil
}

// Close does nothing.
func (s *FileSource) Close() error { return nil }

func parseCSV(r io.Reader) ([]float64, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, err
	}
	col := -1
	var out []float64
	for i, row := range rows {
		if len(row) == 0 {
			continue
		}
		if col < 0 {
			col = len(row) - 1
			if _, err := strconv.ParseFloat(strings.TrimSpace(row[col]), 64); err != nil { // a header row
				for j, h := range row {
					if strings.EqualFold(strings.TrimSpace(h), "price") {
						col = j
					}
				}
				continue
			}
		}
		if col >= len(row) {
			return nil, fmt.Errorf("row %d has no column %d", i+1, col+1)
		}
		p, err := strconv.ParseFloat(strings.TrimSpace(row[col]), 64)
		if err != nil || p <= 0 {
			return nil, fmt.Errorf("row %d: bad price %q", i+1, row[col])
		}
		out = append(out, p)
	}
	return out, nil
}

func parseJSON(r io.Reader) ([]float64, error) {
	var raw []json.RawMessage
	if err := json.NewDecoder(r).Decode(&raw); err != nil {
		return nil, err
	}
	out := make([]float64, 0, len(raw))
	for i, m := range raw {
		var p float64
		if err := json.Unmarshal(m, &p); err != nil {
			var o struct {
				Price json.Number `json:"price"`
			}
			if err := json.Unmarshal(m, &o); err != nil {
				return nil, fmt.Errorf("element %d: %w", i, err)
			}
			if p, err = o.Price.Float64(); err != nil {
				return nil, fmt.Errorf("element %d: bad price", i)
			}
		}
		if p <= 0 {
			return nil, fmt.Errorf("element %d: bad price %v", i, p)
		}
		out = append(out, p)
	}
	return out, nil
}

// CoinbaseURL is the public Coinbase Exchange WebSocket feed (no API key).
const CoinbaseURL = "wss://ws-feed.exchange.coinbase.com"

// CoinbaseSource follows the live ticker of one product. Each Next waits for
// the next ticker message.
type CoinbaseSource struct{ c *websocket.Conn }

// NewCoinbaseSource connects and subscribes to the ticker channel. hc may be
// nil (the default client); tests pass a client for a fake server.
func NewCoinbaseSource(ctx context.Context, url, product string, hc *http.Client) (*CoinbaseSource, error) {
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPClient: hc})
	if err != nil {
		return nil, err
	}
	sub, _ := json.Marshal(map[string]any{"type": "subscribe", "product_ids": []string{product}, "channels": []string{"ticker"}})
	if err := c.Write(ctx, websocket.MessageText, sub); err != nil {
		_ = c.CloseNow()
		return nil, err
	}
	return &CoinbaseSource{c: c}, nil
}

// Next returns the price of the next ticker message.
func (s *CoinbaseSource) Next(ctx context.Context) (float64, error) {
	for {
		_, data, err := s.c.Read(ctx)
		if err != nil {
			return 0, err
		}
		var m struct {
			Type    string `json:"type"`
			Price   string `json:"price"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(data, &m); err != nil {
			continue
		}
		switch m.Type {
		case "error":
			return 0, errors.New("coinbase feed: " + m.Message)
		case "ticker":
			p, err := strconv.ParseFloat(m.Price, 64)
			if err != nil || p <= 0 {
				continue
			}
			return p, nil
		}
	}
}

// Close closes the connection.
func (s *CoinbaseSource) Close() error { return s.c.CloseNow() }
