#!/usr/bin/env bash
# Start the pieces needed to look at the UI, all as local processes:
#   Kafka (compose), the gateway, marketdata (feed, latency endpoint and the page) and the bot.
#
#   scripts/ui-demo.sh up      start everything and print the address (default)
#   scripts/ui-demo.sh down    stop the processes it started
#   scripts/ui-demo.sh status  say what is running
#
#   FEED=coinbase scripts/ui-demo.sh up   take the reference price from the live public Coinbase
#                                         ticker instead of the synthetic file (needs internet;
#                                         PRODUCT=ETH-USD picks another product, default BTC-USD)
#
# The page is then at http://localhost:8083/ . By default the bot replays the synthetic price
# file in data/ in a loop (about 20 steps a second). Either way the orders, trades and the book
# are the bot's own, simulated inside this system: with FEED=coinbase only the reference price
# the bot quotes around is real, not the market. A book of 8 levels a side, no funds needed (the
# gateway runs without Redis risk here).
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=.demo
mkdir -p "$RUN"
GRPC=50081; HTTP=18081; MD=8083
FEED="${FEED:-file}"; PRODUCT="${PRODUCT:-BTC-USD}"
case "$FEED" in file|coinbase) ;; *) echo "FEED must be file or coinbase" >&2; exit 2 ;; esac

alive() { [ -f "$RUN/$1.pid" ] && kill -0 "$(cat "$RUN/$1.pid")" 2>/dev/null; }
stop_all() {
  for n in bot marketdata gateway; do
    if alive "$n"; then kill "$(cat "$RUN/$n.pid")" 2>/dev/null || true; fi
    rm -f "$RUN/$n.pid"
  done
}

case "${1:-up}" in
  down) stop_all; echo "stopped"; exit 0 ;;
  status)
    for n in gateway marketdata bot; do alive "$n" && echo "$n running (pid $(cat "$RUN/$n.pid"))" || echo "$n not running"; done
    exit 0 ;;
  up) ;;
  *) echo "usage: $0 up|down|status" >&2; exit 2 ;;
esac

for t in docker node npm go; do command -v "$t" >/dev/null || { echo "missing tool: $t" >&2; exit 2; }; done
stop_all

echo "== Kafka"
docker compose -f deploy/docker-compose.yml up -d --wait kafka >/dev/null

echo "== building"
go build -o bin/gateway ./cmd/gateway
go build -o bin/marketdata ./cmd/marketdata
go build -o bin/bot ./cmd/bot
(cd ui && { [ -d node_modules ] || npm ci >/dev/null; } && npm run build >/dev/null)

SYMBOL="DEMO$(date +%s)-USD"   # a fresh event log every time
echo "== starting $SYMBOL (reference price: $FEED$([ "$FEED" = coinbase ] && echo " $PRODUCT, live"))"
nohup ./bin/gateway -grpc ":$GRPC" -http ":$HTTP" -symbol "$SYMBOL" -kafka localhost:9092 -data-dir "$RUN/data" >"$RUN/gateway.log" 2>&1 &
echo $! >"$RUN/gateway.pid"
for _ in $(seq 1 100); do curl -fsS "localhost:$HTTP/readyz" >/dev/null 2>&1 && break; sleep 0.2; done

nohup ./bin/marketdata -kafka localhost:9092 -symbol "$SYMBOL" -http ":$MD" \
  -gateway-metrics "http://localhost:$HTTP/metrics" -ui-dir ui/dist >"$RUN/marketdata.log" 2>&1 &
echo $! >"$RUN/marketdata.pid"
for _ in $(seq 1 100); do curl -fsS "localhost:$MD/readyz" >/dev/null 2>&1 && break; sleep 0.2; done

BOT_FEED=(-feed file -loop -interval 50ms)
[ "$FEED" = coinbase ] && BOT_FEED=(-feed coinbase -product "$PRODUCT")   # paced by the ticker messages
nohup ./bin/bot -addr "localhost:$GRPC" -symbol "$SYMBOL" -seed 1 "${BOT_FEED[@]}" -levels 8 -half-spread 4 >"$RUN/bot.log" 2>&1 &
echo $! >"$RUN/bot.pid"
if [ "$FEED" = coinbase ]; then
  # The feed needs the internet: if the bot cannot connect it exits at once, say so instead of
  # leaving an empty page.
  sleep 5
  if ! alive bot; then
    echo "The bot could not follow the Coinbase feed:" >&2
    tail -3 "$RUN/bot.log" >&2
    if grep -qiE "x509|certificate" "$RUN/bot.log"; then
      echo "The TLS certificate was rejected. A DNS filter, proxy or captive page on this network is probably" >&2
      echo "answering for ws-feed.exchange.coinbase.com. This script does not turn certificate checks off;" >&2
      echo "try another network, or run without FEED=coinbase." >&2
    fi
    stop_all
    exit 1
  fi
fi

cat <<MSG

Open  http://localhost:$MD/   (symbol $SYMBOL)
Stop  scripts/ui-demo.sh down
Logs  $RUN/*.log
MSG
