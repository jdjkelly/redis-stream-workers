require 'redis'

r = Redis.new

i = 0
1_000_000.times do
  r.xadd("orders", "*", "order-id", i, "state", "unprocessed")
  i += 1
end
