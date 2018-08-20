require './worker'

def process_message(id, msg)
  if msg[3] == "unprocessed"
    # puts "[#{ConsumerName}] #{id} = #{msg.inspect}"

    redis.multi do
      redis.xadd('unprocessed-orders', '*', 'order-id', id)
      redis.xack(Stream, GroupName, id)
    end
  end
end

threads = []

4.times do |n|
  threads << Thread.new do
    work(GroupName, "#{ConsumerName}-#{Process.pid}-#{n}", Stream) do |id, msg|
      process_message(id, msg)
    end
  end
end

threads.each { |thr| thr.join }
