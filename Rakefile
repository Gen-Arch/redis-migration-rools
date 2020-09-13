require "redis"
require "tomlrb"

CONFIG = Tomlrb.load_file('config/redis.toml', symbolize_keys: true)

@src = Redis.new(**CONFIG[:src])
@dst = Redis.new(**CONFIG[:dst])

desc "add test data"
task :add_test_data do
  1000.times do |i|
    @src.set i, "hello#{i}"
  end
end

desc "delete"
task :delete do
  @dst.keys do |key|
    @dst.del(key)
  end
end

desc "show keys"
task :keys do
  pp "src:#{@src.keys.size}"
  pp "dst:#{@dst.keys.size}"
end

desc "migration"
task :migration do
  @src.keys.each do |key|
    v = @src.get(key)
    @dst.set(key, v)
    puts "add => key: #{key}, value: #{v}"
  end
end
