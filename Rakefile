require "redis"
require "tomlrb"

CONFIG          = Tomlrb.load_file('config/redis.toml', symbolize_keys: true)
TYPES           = [:src, :dst]
TEST_DATA_COUNT = 1000

@redis = Hash.new

TYPES.each do |type|
  @redis[type] = Redis.new(**CONFIG[type])
end

namespace :show do
  namespace :keys do
    desc "show keys all"
    task :all => TYPES

    TYPES.each do |type|
      task type do
        pp "#{type.to_s}:#{@redis[type].keys.size}"
      end
    end
  end
end

desc "add test data"
task :add_test_data do
  TEST_DATA_COUNT.times do |i|
    @redis[:src].set i, "hello#{i}"
  end
end


namespace :delete do
  TYPES.each do |type|
    keys = @redis[type].keys

    namespace type do
      desc "delete"
      task :all => keys

      keys.each do |key|
        task key do
          @redis[type].del(key)
        end
      end
    end
  end
end

desc "migration"
task :migration do
  @redis[:src].keys.each do |key|
    v = @redis[:src].get(key)
    @redis[:dst].set(key, v)
    puts "add => key: #{key}, value: #{v}"
  end
end

namespace :check do
  desc "check data"
  task :value do
    result = Array.new
    @redis[:src].keys.each do |key|
      src = @redis[:src].get(key)
      dst = @redis[:dst].get(key)

      if src == dst
        puts "OK => key(#{key})" if ENV["varbose"]
        result << true
      else
        puts "NG => key(#{key})" if ENV["varbose"]
        result << false
      end
    end
    puts "result: #{result.all? ? 'OK' : 'NG'}"
  end
end
