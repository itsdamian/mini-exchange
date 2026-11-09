package risk

import "github.com/redis/go-redis/v9"

// Redis Lua numbers are IEEE doubles, so integers are exact only below 2^53.
// Every amount that reaches a script is bounded far below that (see
// MaxBalance and the Limits validation), and balances are conserved by
// settlement, so sums stay in range.
//
// Key layout, per account (the {id} hash tag keeps them in one cluster slot):
//
//	{id}:bal        hash  b_avail b_held q_avail q_held (base / quote units),
//	                      open (orders holding a reservation), tok ts (token bucket)
//	{id}:rc:<cid>   hash  side held px   reservation before the engine accepted the order
//	{id}:ro:<oid>   hash  side held px   reservation of an accepted order
//	{id}:seq:<sym>  int   last engine event sequence applied for the symbol (settlement dedup)
//
// The settle script builds the reservation keys itself from the {id} prefix.
// They are not declared as KEYS, which Redis permits outside cluster mode and
// which stays within one slot because of the hash tag.

// reserveScript atomically takes a rate-limit token, checks the open-order
// limit and the available balance, and moves the funds from available to held.
//
// KEYS: bal, res
// ARGV: now_us, rate_per_sec, burst, max_open, side(B|S), cost, px
// Returns 0 ok (also when the reservation already exists), 1 rate limited,
// 2 too many open orders, 3 insufficient funds.
var reserveScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end

local avail, held
if ARGV[5] == 'B' then avail, held = 'q_avail', 'q_held' else avail, held = 'b_avail', 'b_held' end
local b = redis.call('HMGET', KEYS[1], 'tok', 'ts', 'open', avail)

local rate = tonumber(ARGV[2])
if rate > 0 then
  local burst = tonumber(ARGV[3]) * 1000000          -- micro-tokens
  local now = tonumber(ARGV[1])
  local tok, ts = tonumber(b[1]), tonumber(b[2])
  if tok == nil then tok, ts = burst, now end
  if now > ts then
    tok = math.min(burst, tok + (now - ts) * rate)   -- 1 token = 1e6 micro-tokens, rate tokens/s = rate micro-tokens/us
    ts = now
  end
  local ok = tok >= 1000000
  if ok then tok = tok - 1000000 end
  redis.call('HSET', KEYS[1], 'tok', string.format('%.0f', tok), 'ts', string.format('%.0f', ts))
  if not ok then return 1 end
end

local max_open = tonumber(ARGV[4])
if max_open > 0 and tonumber(b[3] or '0') >= max_open then return 2 end

local cost = tonumber(ARGV[6])
if tonumber(b[4] or '0') < cost then return 3 end

redis.call('HINCRBY', KEYS[1], avail, '-' .. ARGV[6])
redis.call('HINCRBY', KEYS[1], held, ARGV[6])
redis.call('HINCRBY', KEYS[1], 'open', 1)
redis.call('HSET', KEYS[2], 'side', ARGV[5], 'held', ARGV[6], 'px', ARGV[7])
return 0
`)

// settleScript applies a run of settlement ops for ONE account in order, in a
// single round trip (one Lua call per account per batch, not per op).
//
// Each op is five ARGV values: kind, event_seq, ref, price, qty.
//
//	kind 1 bind     ref = client id, price = order id: move the reservation
//	                from the client id to the order id.
//	kind 2 fill     ref = order id: a buyer spends qty*price quote (the
//	                difference to its reserved price is released) and gains qty
//	                base; a seller spends qty base and gains qty*price quote.
//	                A fully consumed reservation is deleted.
//	kind 3 cancel   ref = order id: release what is still held, delete.
//	kind 4 release  ref = client id: same, for a reservation never accepted.
//
// An op whose event_seq is not above the last applied one is a duplicate and
// is skipped; event_seq 0 is never deduplicated (the gateway's own release),
// and is still idempotent because the reservation is deleted.
//
// KEYS: bal, seq   ARGV[1]: the {id}: key prefix, then the ops.
// Returns one code per op: 0 ok, 1 duplicate, 2 reservation missing, 3 fill
// exceeds the reservation (nothing changed: an invariant violation to alert on).
var settleScript = redis.NewScript(`
local pre = ARGV[1]
local last = tonumber(redis.call('GET', KEYS[2]) or '0')
local top = last
local out = {}

local function release(res)
  local r = redis.call('HMGET', res, 'side', 'held')
  if not r[1] then return 2 end
  if r[1] == 'B' then
    redis.call('HINCRBY', KEYS[1], 'q_held', '-' .. r[2])
    redis.call('HINCRBY', KEYS[1], 'q_avail', r[2])
  else
    redis.call('HINCRBY', KEYS[1], 'b_held', '-' .. r[2])
    redis.call('HINCRBY', KEYS[1], 'b_avail', r[2])
  end
  redis.call('DEL', res)
  redis.call('HINCRBY', KEYS[1], 'open', -1)
  return 0
end

for i = 2, #ARGV, 5 do
  local kind, s, ref = tonumber(ARGV[i]), tonumber(ARGV[i + 1]), ARGV[i + 2]
  local code = 0
  if s > 0 and s <= last then
    code = 1
  else
    if s > top then top = s end
    if kind == 1 then
      local rc = pre .. 'rc:' .. ref
      if redis.call('EXISTS', rc) == 0 then code = 2
      else redis.call('RENAME', rc, pre .. 'ro:' .. ARGV[i + 3]) end
    elseif kind == 2 then
      local res = pre .. 'ro:' .. ref
      local r = redis.call('HMGET', res, 'side', 'held', 'px')
      if not r[1] then
        code = 2
      else
        local p, q, held = tonumber(ARGV[i + 3]), tonumber(ARGV[i + 4]), tonumber(r[2])
        if r[1] == 'B' then
          local hold, spend = q * tonumber(r[3]), q * p
          if hold > held or spend > hold then
            code = 3
          else
            redis.call('HINCRBY', KEYS[1], 'q_held', -hold)
            redis.call('HINCRBY', KEYS[1], 'q_avail', hold - spend)
            redis.call('HINCRBY', KEYS[1], 'b_avail', q)
            held = held - hold
          end
        elseif q > held then
          code = 3
        else
          redis.call('HINCRBY', KEYS[1], 'b_held', -q)
          redis.call('HINCRBY', KEYS[1], 'q_avail', q * p)
          held = held - q
        end
        if code == 0 then
          if held == 0 then
            redis.call('DEL', res)
            redis.call('HINCRBY', KEYS[1], 'open', -1)
          else
            redis.call('HSET', res, 'held', string.format('%.0f', held))
          end
        end
      end
    elseif kind == 3 then
      code = release(pre .. 'ro:' .. ref)
    else
      code = release(pre .. 'rc:' .. ref)
    end
    if s > 0 then last = s end
  end
  out[#out + 1] = code
end
if top > tonumber(redis.call('GET', KEYS[2]) or '0') then redis.call('SET', KEYS[2], top) end
return out
`)

var allScripts = []*redis.Script{reserveScript, settleScript}
