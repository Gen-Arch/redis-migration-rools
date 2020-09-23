def get_con
  redis = Hash.new
  TYPES.map do |type|
    redis[type] = Redis.new(**CONFIG[type])
  end

  redis
end

def parse(data)
  data.force_encoding('UTF-8')
  data = data.encode("UTF-16BE", "UTF-8", :invalid => :replace, :undef => :replace, :replace => '?').encode("UTF-8")
  data = data.split("\s")

  data =
    case size = data.size
    when size == 1
      {type: data}
    when size == 3
      {
        id:     data[0],
        src:    "#{data[1]} #{data[2]}",
        type:   data[3].gsub(/^\"|\"$/, ""),
      }
    when size >= 6
      {
        id:     data[0],
        src:    "#{data[1]} #{data[2]}",
        type:   data[3].gsub(/^\"|\"$/, ""),
        key:    data[4].gsub(/^\"|\"$/, ""),
        value:  data[5...-1].gsub(/^\"|\"$/, "")
      }
    end
end

def get_logger
  loggers = Array.new
  loggers = Logger.new(STDOUT,     datetime_format: '%Y-%m-%d %H:%M:%S')
  loggers = Logger.new(ENV["log"], datetime_format: '%Y-%m-%d %H:%M:%S') if ENV["log"]
end
