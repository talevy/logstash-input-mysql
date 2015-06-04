require "logstash/devutils/rspec/spec_helper"
require "logstash/plugin"
require "jdbc/mysql"
require "sequel"
require "sequel/adapters/jdbc"
require "docker_doctor"

describe "tests" do
  it "hello_world" do
    @docker = DockerDoctor.new
    @docker.provision("ls-mysql", "spec/integration/docker/Dockerfile")
    @db = {
      :host => @docker.host[:host_ip],
      :port => @docker.host[:ports]['3306/tcp'][0]['HostPort']
    }
    sleep 13

    Jdbc::MySQL.load_driver

    settings = {
      "host" => @db[:host],
      "port" => @db[:port],
      "database" => "testdb",
      "user" => "logstash",
      "password" => "logstash",
      "statement" => "SELECT * FROM foo where created_at >= :sql_last_start"
    }

    database = Sequel.connect("jdbc:mysql://#{@db[:host]}:#{@db[:port]}/#{settings['database']}",
                              :user=> settings['user'], :password=> settings['password'])
    database.create_table :foo do
      DateTime :created_at
      Integer :num
    end
    test_table = database[:foo]

    # test

    plugin = LogStash::Plugin.lookup("input", "mysql").new(settings)
    plugin.register

    q = Queue.new

    nums = [10, 20, 30, 40, 50]
    plugin.run(q)
    test_table.insert(:num => nums[0], :created_at => Time.now.utc)
    test_table.insert(:num => nums[1], :created_at => Time.now.utc)
    sleep 1
    plugin.run(q)
    test_table.insert(:num => nums[2], :created_at => Time.now.utc)
    test_table.insert(:num => nums[3], :created_at => Time.now.utc)
    test_table.insert(:num => nums[4], :created_at => Time.now.utc)
    sleep 1
    plugin.run(q)

    actual_sum = 0
    until q.empty? do
      actual_sum += q.pop['num']
    end

    plugin.teardown

    insist { actual_sum } == nums.inject{|sum,x| sum + x }

    @docker.cleanup
  end
end
