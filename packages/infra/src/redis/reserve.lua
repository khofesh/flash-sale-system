-- Atomic reservation (SPEC §5.2).
-- KEYS[1] sale:{id}:meta   KEYS[2] sale:{id}:stock
-- KEYS[3] sale:{id}:buyers KEYS[4] sale:{id}:reservations
-- ARGV[1] userId           ARGV[2] reservationId
--
-- The sale window is checked against Redis TIME, the same clock every replica
-- reads, so host clock drift cannot move the window.

local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)

local meta = redis.call('HMGET', KEYS[1], 'startsAt', 'endsAt')
local startsAt = tonumber(meta[1])
local endsAt = tonumber(meta[2])
if not startsAt or not endsAt then
  return {'NOT_FOUND'}
end

local stock = redis.call('GET', KEYS[2])
if not stock then
  -- The sale is configured but its stock is gone. Fail closed instead of
  -- treating it as sold out or, worse, refilling it.
  return {'UNAVAILABLE'}
end

if now < startsAt then
  return {'NOT_STARTED'}
end
if now >= endsAt then
  return {'ENDED'}
end

if redis.call('SISMEMBER', KEYS[3], ARGV[1]) == 1 then
  return {'ALREADY_PURCHASED'}
end

if tonumber(stock) <= 0 then
  return {'SOLD_OUT'}
end

redis.call('DECR', KEYS[2])
redis.call('SADD', KEYS[3], ARGV[1])
redis.call('XADD', KEYS[4], '*', 'reservationId', ARGV[2], 'userId', ARGV[1], 'ts', now)

return {'RESERVED', ARGV[2]}
