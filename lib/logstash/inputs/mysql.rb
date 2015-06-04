require 'logstash/namespace'
require 'logstash/inputs/base'
require 'logstash/inputs/jdbc'
require "jdbc/mysql"

class LogStash::Inputs::Mysql < LogStash::Inputs::Base
  config_name 'mysql'

  default :codec, "plain"

  config :host, :validate => :string, :default => "localhost"

  config :port, :validate => :number, :default => 3306

  config :database, :validate => :string, :required => true

  config :user, :validate => :string, :required => true

  config :password, :validate => :string

  config :statement, :validate => :string, :required => true

  config :parameters, :validate => :hash, :default => {}

  config :schedule, :validate => :string

  def register
    Jdbc::MySQL.load_driver
    @jdbc_plugin = LogStash::Inputs::Jdbc.new({
      "jdbc_driver_class" => "com.mysql.jdbc.Driver",
      "jdbc_connection_string" => "jdbc:mysql://#{@host}:#{@port}/#{@database}",
      "jdbc_user" => @user,
      "jdbc_password" => @password,
      "statement" => @statement,
      "parameters" => @parameters,
      "schedule" => @schedule
    })

    @jdbc_plugin.register
  end

  def run(queue)
    @jdbc_plugin.run(queue)
  end

  def teardown
    @jdbc_plugin.teardown
  end
end
