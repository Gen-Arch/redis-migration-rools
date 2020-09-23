require "redis"
require "logger"
require 'securerandom'

require "./lib/redis_migration.rb"

TYPES           = [:src, :dst]
CONFIG          = ENV["file"]
TEST_DATA_COUNT = 1000

@redismig = RedisMigration.new(CONFIG)

TYPES.each do |type|
  namespace :keys do
    desc "show keys all"
    task :all => TYPES

    task type do
      @redismig.put_keys(type)
    end
  end

  namespace :ttl do
    desc "show ttl all"
    task :all => TYPES

    task type do
      @redismig.ttls(type)
    end
  end
end

desc "add test data"
task :add_test_data do
  if ENV["loop"]
    loop do
      @redismig.add_test_data
      sleep 5
    end
  else
    TEST_DATA_COUNT.times do
      @redismig.add_test_data
    end
  end
end

namespace :delete do
  desc "delete all data"
  task :all => TYPES
  TYPES.each do |type|
    task type do
      @redismig.del(type)
    end
  end
end

desc "migration"
task :migration do
  @redismig.migration
end

namespace :check do
  desc "check data"
  task :value do
    puts "result: #{@redismig.check ? 'OK' : 'NG'}"
  end
end

desc "Data synchronization"
task :sync do
  @redismig.sync
end

desc "Data synchronization confirmation"
task :sync_mon do
  loop do
    @redismig.sync?
    sleep 1
  end
end

desc "Investigate the data"
task :watch do
  @redismig.watch
end
