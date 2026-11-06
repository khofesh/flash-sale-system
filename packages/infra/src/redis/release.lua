-- Idempotent compensation (SPEC §4.2, §5.2).
-- KEYS[1] sale:{id}:buyers KEYS[2] sale:{id}:stock
-- ARGV[1] userId
--
-- Only a user that is still in the buyers set releases a unit. Running this
-- twice is a no-op: the second call finds no buyer and does not INCR again.

if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 1 then
  redis.call('SREM', KEYS[1], ARGV[1])
  redis.call('INCR', KEYS[2])
  return 1
end

return 0
