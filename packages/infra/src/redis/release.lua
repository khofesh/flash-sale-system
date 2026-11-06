-- Idempotent compensation (SPEC §4.2, §5.2).
-- KEYS[1] sale:{id}:buyers KEYS[2] sale:{id}:stock KEYS[3] sale:{id}:holders
-- ARGV[1] userId           ARGV[2] reservationId
--
-- Only the reservation that currently holds the user's unit releases it. Running
-- this twice is a no-op, and so is a stale release for an older order after the
-- user bought again: neither matches the holder, so neither INCRs.
--
-- Without a stock key the sale is awaiting a rebuild. Return -1 rather than 0, so
-- the caller does not record a release that never happened, and never INCR: that
-- would create a stock key and reopen a sale that must stay closed.

if redis.call('EXISTS', KEYS[2]) == 0 then
  return -1
end

if redis.call('HGET', KEYS[3], ARGV[1]) == ARGV[2] then
  redis.call('HDEL', KEYS[3], ARGV[1])
  redis.call('SREM', KEYS[1], ARGV[1])
  redis.call('INCR', KEYS[2])
  return 1
end

return 0
