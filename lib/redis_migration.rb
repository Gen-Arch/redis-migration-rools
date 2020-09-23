require "redis"
require "tomlrb"
require "logger"
require "colorize"

class RedisMigration
  UPDATE_COMMANDS       = /set|setex|getset/i
  UPDATE_MSEC_COMMANDS  = /psetex/i
  DELETE_COMMANDS       = /del/i

  def initialize(file)
    file    ||= 'config/redis.toml'
    @config  = Tomlrb.load_file(file, symbolize_keys: true)
    @loggers = get_logger
    @redis   = connection
  end

  def connection
    types = [:src, :dst]
    redis = Hash.new

    types.each do |type|
      redis[type] = Redis.new(**@config[type])
    end

    redis
  end


  def keys(type)
    @redis[type].keys
  end

  def ttl(type)
    keys(type).each{|key| puts "key: #{key} => ttl: #{@redis[type].ttl(key)}"}
  end

  def put_keys(type)
    puts "#{type.to_s}:#{keys(type).size}"
  end

  def get(type)
    data = Hash.new
    keys(type).each do |key|
      data[key] = @redis[type].get(key)
    end

    data
  end


  def del(type)
    keys(type).each do |key|
      @redis[type].del(key)
      puts "delete => key : #{key}"
    end
  end

  def migration
    keys(:src).each do |key|
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

  def check
    s = get(:src)
    d = get(:dst)

    return s.sort == d.sort
  end

  def sync
    sync_src = Redis.new(**@config[:src])
    sync_dst = Redis.new(**@config[:dst])

    @redis[:src].monitor do |data|
      data = parse(data)

      case data[:type]
      when UPDATE_COMMANDS
        v   = sync_src.get(data[:key])
        ttl = sync_src.ttl(data[:key])

        sync_dst.setex(data[:key], ttl, v)
        puts "sync => key: #{data[:key]} ttl: #{ttl}"
      when UPDATE_MSEC_COMMANDS
        v   = sync_src.get(data[:key])
        ttl = sync_src.pttl(data[:key])

        sync_dst.psetex(data[:key], ttl, v)
        puts "sync => key: #{data[:key]} ttl: #{ttl}msec"
      when DELETE_COMMANDS
        sync_dst.del(data[:key])
      else
        next
      end
    end
  end

  def sync?
    src = @redis[:src].keys
    dst = @redis[:dst].keys
    result = src.sort == dst.sort
    status = result ? "OK!!".colorize(:green) : "NG!!".colorize(:red)

    @loggers.each do |logger|
      logger.info "sync: #{status} => src: #{src.size} dst: #{dst.size}"
    end
  end

  def watch
    @redis[:src].monitor do |data|
      data = parse(data)

      data.delete(:value) unless ENV["varbose"] && data[:value]

      @loggers.each do |logger|
        logger.info(data)
      end
    end
  end

  def add_test_data
    key   = SecureRandom.alphanumeric
    value = Array.new
    rand(100).times do
      value << SecureRandom.base64(1000)
    end

    @redis[:src].setex(key, rand(99999), value.join(" "))
    pp @redis[:src].get(key)
  end

  private
  def parse(data)
    data.force_encoding('UTF-8')
    data = data.encode("UTF-16BE", "UTF-8", :invalid => :replace, :undef => :replace, :replace => '?').encode("UTF-8")
    data = data.split("\s")

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
      Hash.new
    end
  end

  def get_logger
    loggers = Array.new
    loggers << Logger.new(STDOUT,     datetime_format: '%Y-%m-%d %H:%M:%S')
    loggers << Logger.new(ENV["log"], datetime_format: '%Y-%m-%d %H:%M:%S') if ENV["log"]

    loggers
  end
end
