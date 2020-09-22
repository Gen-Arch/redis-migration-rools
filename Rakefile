require "redis"
require "tomlrb"
require "logger"
require "colorize"
require 'securerandom'

CONFIG          = Tomlrb.load_file('config/redis.toml', symbolize_keys: true)
TYPES           = [:src, :dst]
TEST_DATA_COUNT = 1000
UPDATE_COMMANDS = /set/

@redis  = Hash.new
@keys   = Hash.new
@logger = Logger.new(STDOUT, datetime_format: '%Y-%m-%d %H:%M:%S')

TYPES.each do |type|
  @redis[type] = Redis.new(**CONFIG[type])
  @keys[type]  = @redis[type].keys
end

namespace :show do
  namespace :keys do
    desc "show keys all"
    task :all => TYPES

    TYPES.each do |type|
      task type do
        puts "#{type.to_s}:#{@redis[type].keys.size}"
      end
    end
  end

  namespace :ttl do
    TYPES.each do |type|
      namespace type do
        desc "show ttl all"
        task :all => @keys[type]

        @keys[type].each do |key|
          task key do
            puts "key: #{key} => ttl: #{@redis[type].ttl(key)}"
          end
        end
      end
    end
  end
end

desc "add test data"
task :add_test_data do
  TEST_DATA_COUNT.times do |i|
    key   = SecureRandom.alphanumeric
    value = SecureRandom.base64(1000)

    @redis[:src].set(key, value)
  end

  if ENV["loop"]
    loop do
      key   = SecureRandom.alphanumeric
      value = SecureRandom.base64(1000)

      @redis[:src].set(key, value)
      p key
      sleep 1
    end
  end
end


namespace :delete do
  TYPES.each do |type|
    namespace type do
      desc "delete all data"
      task :all => @keys[type]

      @keys[type].each do |key|
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

desc "Data synchronization"
task :sync do
  @redis[:src].monitor do |src|
    next unless src = parse_monitor(src)

    if src[:type] =~ UPDATE_COMMANDS
      @redis[:dst].set(src[:key], src[:value])
      puts "sync => #{src}"
    end
  end
end

desc "Data synchronization confirmation"
task :sync_mon do
  loop do
    src = @redis[:src].keys
    dst = @redis[:dst].keys
    if src.sort == dst.sort
      status = "OK!!".colorize(:green)
    else
      status = "NG!!".colorize(:red)
    end
    @logger.info "sync: #{status} => src: #{src.size} dst: #{dst.size}"
    sleep 1
  end
end

desc "Investigate the data"
task :watch do
  logger = Logger.new("log/monitor-#{Time.now.strftime("%Y-%m-%dT%H%M%S")}.log", datetime_format: '%Y-%m-%d %H:%M:%S')
  @redis[:src].monitor do |src|
    logger.info(src)
  end
end

def parse_monitor(data)
  data = data.split("\s")
  return nil unless data.size == 6
  data = {
    id:     data[0],
    src:    "#{data[1]} #{data[2]}",
    type:   data[3].gsub(/^\"|\"$/, ""),
    key:    data[4].gsub(/^\"|\"$/, ""),
    value:  data[5].gsub(/^\"|\"$/, "")
  }
end
