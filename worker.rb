require 'redis'

if ARGV.length < 3
    puts "please specify [CONSUMERNAME] [GROUPNAME] [STREAM]"
    exit 1
end

GroupName = ARGV[0]
ConsumerName = ARGV[1]
Stream  = ARGV[2]

def redis
  @redis ||= Redis.new
end

$lastid = '0-0'

def work(group_name, consumer_name, stream)
  puts "Consumer #{ConsumerName} starting..."
  check_backlog = true
  while true
      # Pick the ID based on the iteration: the first time we want to
      # read our pending messages, in case we crashed and are recovering.
      # Once we consumer our history, we can start getting new messages.
      if check_backlog
          myid = $lastid
      else
          myid = '>'
      end

      items = redis.xreadgroup('GROUP', group_name, consumer_name, 'BLOCK', '2000', 'COUNT', '1', 'STREAMS', stream, myid)

      if items == nil
          puts "#{consumer_name} done"
          next
      end

      # If we receive an empty reply, it means we were consuming our history
      # and that the history is now empty. Let's start to consume new messages.
      check_backlog = false if items[0][1].length == 0

      items[0][1].each{|i|
          id,fields = i

          yield(id, fields)

          $lastid = id
      }
  end
end
