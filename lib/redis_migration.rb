require "redis"
require "tomlrb"
require "logger"
require "colorize"

class RedisMigration
  UPDATE_COMMANDS       = /set|setex|getset/i
  UPDATE_MSEC_COMMANDS  = /psetex/i
  DELETE_COMMANDS       = /del/i

  def initialize(file)
    file        ||= 'sample/redis.toml'
    @config     = Tomlrb.load_file(file, symbolize_keys: true)
    @option     = @config[:option]
    @loggers    = get_logger
    @read_con   = connection
    @write_con  = connection
    @ignore_key = @option && @option[:ignore_key] ? @option[:ignore_key] : Array.new
  end

  def ignore_key(*key)
    @ignore_key.concat(key)
  end

  def keys(type)
    @read_con[type].keys.map do |key|
      next if @ignore_key.include?(key)
      key
    end.compact
  end

  def put_keys(type)
    puts "#{type.to_s}:#{keys(type).size}"
  end

  def ttl(type, key)
    @read_con[type].ttl(key)
  end

  def ttls(type)
    keys(type).each{|key| puts "#{type.to_s} => key: #{key} ttl: #{ttl(type, key)}"}
  end

  def check
    s = values(:src).sort
    d = values(:dst).sort


    return s == d
  end

  def diff
    s = values(:src).sort
    d = values(:dst).sort

    s_diff = s - d
    d_diff = d - s

    {
      src: s_diff,
      dst: d_diff
    }
  end

  def del(type)
    keys(type).each do |key|
      @redis[type].del(key)
      puts "delete => key : #{key}"
    end
  end

  def migration
    keys(:src).each do |key|
      _, ttl = set({:src => :dst}, key)
      puts "add => key: #{key}, ttl: #{ttl}"
    end
  end

  def sync
    @read_con[:src].monitor do |data|
      data = parse(data)

      case data[:type]
      when UPDATE_COMMANDS
        _, ttl = set({:src => :dst}, data[:key])
        puts "sync => key: #{data[:key]} ttl: #{ttl}"
      when UPDATE_MSEC_COMMANDS
        _, ttl = pset({:src => :dst}, data[:key])
        puts "sync => key: #{data[:key]} ttl: #{ttl}msec"
      when DELETE_COMMANDS
        @write_con[:dst].del(data[:key])
        puts "delete => key: #{data[:key]}"
      else
        next
      end
    end
  end

  def sync?
    src    = keys(:src).sort
    dst    = keys(:dst).sort
    result = src == dst
    status = result ? "OK!!".colorize(:green) : "NG!!".colorize(:red)

    @loggers.each do |logger|
      logger.info "sync: #{status} => src: #{src.size} dst: #{dst.size}"
    end
  end

  def watch
    @read_con[:src].monitor do |data|
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
  def connection
    types = [:src, :dst]
    redis = Hash.new
    types.each do |type|
      redis[type] = Redis.new(**@config[type])
    end

    redis
  end

  def values(type)
    data = Hash.new
    keys(type).each do |key|
      data[key] = @read_con[type].get(key)
    end

    data
  end

  def set(direction, key)
    src, dst = direction.first

    v   = @write_con[src].get(key)
    ttl = @write_con[src].ttl(key)

    # not key
    return [v, ttl] if ttl == -2

    if ttl == -1
      @write_con[dst].set(key, v)
    else
      @write_con[dst].setex(key, ttl, v)
    end

    [v, ttl]
  end

  def pset(direction, key)
    src, dst = direction.first

    v   = @write_con[src].get(key)
    ttl = @write_con[src].pttl(key)

    # not key
    return [v, ttl] if ttl == -2

    if ttl == -1
      @write_con[dst].set(key, v)
    else
      @write_con[dst].psetex(key, ttl, v)
    end

    [v, ttl]
  end

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
