require "redis"
require "tomlrb"
require "logger"
require "colorize"
require 'securerandom'

CONFIG_FILE           = ENV["file"] || 'config/redis.toml'
CONFIG                = Tomlrb.load_file(CONFIG_FILE, symbolize_keys: true)
TYPES                 = [:src, :dst]
TEST_DATA_COUNT       = 1000
UPDATE_COMMANDS       = /set|setex|getset/i
UPDATE_MSEC_COMMANDS  = /psetex/i
DELETE_COMMANDS       = /del/i

def get_con
  redis = Hash.new
  TYPES.map do |type|
    redis[type] = Redis.new(**CONFIG[type])
  end
  redis
end

def get_logger
  loggers = Array.new
  loggers << Logger.new(STDOUT,     datetime_format: '%Y-%m-%d %H:%M:%S')
  loggers << Logger.new(ENV["log"], datetime_format: '%Y-%m-%d %H:%M:%S') if ENV["log"]

  return loggers
end

def parse(data)
  data.force_encoding('UTF-8')
  data = data.encode("UTF-16BE", "UTF-8", :invalid => :replace, :undef => :replace, :replace => '?').encode("UTF-8")
  data = data.split("\s")

  data =
    if data.size == 1
      {type: data}
    elsif data.size == 4
      {
        id:     data[0],
        src:    "#{data[1]} #{data[2]}",
        type:   data[3].gsub(/^\"|\"$/, ""),
      }
    elsif data.size == 5
      {
        id:     data[0],
        src:    "#{data[1]} #{data[2]}",
        type:   data[3].gsub(/^\"|\"$/, ""),
        key:    data[4].gsub(/^\"|\"$/, ""),
      }
    elsif data.size >= 6
      {
        id:     data[0],
        src:    "#{data[1]} #{data[2]}",
        type:   data[3].gsub(/^\"|\"$/, ""),
        key:    data[4].gsub(/^\"|\"$/, ""),
        value:  data[5...-1].join(" ").gsub(/^\"|\"$/, "")
      }
    else
      pp data
      Hash.new
    end
end

@redis   = get_con
@loggers = get_logger

TYPES.each do |type|
  namespace :keys do
    desc "show keys all"
    task :all => TYPES

    task type do
      puts "#{type.to_s}:#{@redis[type].keys.size}"
    end
  end

  namespace :ttl do
    desc "show ttl all"
    task :all => TYPES

    task type do
      keys = @redis[type].keys
      keys.each{|key| puts "key: #{key} => ttl: #{@redis[type].ttl(key)}"}
    end
  end
end

desc "add test data"
task :add_test_data do
  TEST_DATA_COUNT.times do |i|
    key   = SecureRandom.alphanumeric
    value = Array.new
    rand(100).times do
      value << SecureRandom.base64(1000)
    end

    @redis[:src].setex(key, rand(99999), value.join(" "))
    pp @redis[:src].get(key)
  end

  if ENV["loop"]
    loop do
      key   = SecureRandom.alphanumeric
      value = Array.new
      rand(100).times do
        value << SecureRandom.base64(1000)
      end

      @redis[:src].setex(key, rand(99999), value.join(" "))
      pp @redis[:src].get(key)
      sleep 5
    end
  end
end

namespace :delete do
  TYPES.each do |type|
    namespace type do
      keys = @redis[type].keys
      desc "delete all data"
      task :all => keys

      keys.each do |key|
        task key do
          @redis[type].del(key)
          puts "delete => key: #{key}"
        end
      end
    end
  end
end

desc "migration"
task :migration do
  @redis[:src].keys.each do |key|
    v   = @redis[:src].get(key)
    ttl = @redis[:src].ttl(key)

    if ttl <= 0
      @redis[:dst].set(key, v)
    else
      @redis[:dst].setex(key, ttl, v)
    end
    puts "add => key: #{key}, ttl: #{ttl}"
  end
end

namespace :check do
  desc "check data"
  task :value do
    result = Hash.new
    TYPES.each do |type|
      result[type] = Hash.new

      @redis[type].keys.each do |key|
        result[type][key] = @redis[type].get(key)
      end
    end

    result = result[:src].sort == result[:dst].sort
    puts "result: #{result ? 'OK' : 'NG'}"
  end
end

desc "Data synchronization"
task :sync do
  redis = get_con

  @redis[:src].monitor do |src|
    src = parse(src)

    case src[:type]
    when UPDATE_COMMANDS
      v   = redis[:src].get(src[:key])
      ttl = redis[:src].ttl(src[:key])

      redis[:dst].setex(src[:key], ttl, v)
      puts "sync => key: #{src[:key]} ttl: #{ttl}"
    when UPDATE_MSEC_COMMANDS
      v   = redis[:src].get(src[:key])
      ttl = redis[:src].pttl(src[:key])

      redis[:dst].psetex(src[:key], ttl, v)
      puts "sync => key: #{src[:key]} ttl: #{ttl}msec"
    when DELETE_COMMANDS
      redis[:dst].del(src[:key])
    else
      next
    end
  end
end

desc "Data synchronization confirmation"
task :sync_mon do
  loop do
    src = @redis[:src].keys
    dst = @redis[:dst].keys

    status = src.sort == dst.sort ? "OK!!".colorize(:green) : "NG!!".colorize(:red)

    @loggers.each do |logger|
      logger.info "sync: #{status} => src: #{src.size} dst: #{dst.size}"
    end
    sleep 1
  end
end

desc "Investigate the data"
task :watch do
  @redis[:src].monitor do |src|
    src = parse(src)

    src.delete(:value) unless ENV["varbose"] && src[:value]

    @loggers.each do |logger|
      logger.info(src)
    end
  end
end



