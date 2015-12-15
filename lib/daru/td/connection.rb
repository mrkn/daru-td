require 'daru'
require 'td'
require 'td-client'

module Daru
  module TD
    class Connection
      def initialize(apikey=nil, endpoint=nil, **kwargs)
        if apikey.nil? && kwargs[:apikey]
          apikey = kwargs.delete(:apikey)
        end

        if endpoint
          unless endpoint.end_with?('/')
            endpoint = endpoint + '/'
          end
          kwargs[:endpoint] = endpoint
        end

        if kwargs[:user_agent].nil?
          versions = [
            "daru/#{Daru::VERSION}",
            "td-client/#{::TD::Client::VERSION}",
            "ruby/#{RUBY_VERSION}"
          ]
          kwargs[:user_agent] = "daru-td/#{Daru::TD::VERSION} (#{versions.join(' ')})"
        end

        @kwargs = kwargs
        @client = get_client(apikey, **kwargs)
      end

      attr_reader :client

      # @return [String] TreasureData API key
      def apikey
        @client.apikey
      end

      def databases
        if (databases = self.client.databases())
          fields = [:name, :count, :permission, :created_at, :updated_at]
          make_dataframe(databases, [:name, :count, :permission, :created_at, :updated_at]) do |db|
            [db.name, db.count, db.permission, db.created_at, db.updated_at]
          end
        else
          Daru::DataFrame.new()
        end
      end

      def tables(database)
        if (tables = self.client.tables(database))
          fields = [:name, :count, :estimated_storage_size, :last_log_timestamp, :created_at]
          make_dataframe(tables, [:name, :count, :estimated_storage_size, :last_log_timestamp, :created_at]) do |t|
            [t.name, t.count, t.estimated_storage_size, t.last_log_timestamp, t.created_at]
          end
        else
          Daru::DataFrame.new()
        end
      end

      def query_engine(database, **kwargs)
      end

      private

      def get_client(apikey, **kwargs)
        ::TD::Client.new(apikey, **kwargs)
      end

      def make_dataframe(enum, fields)
        vectors = Hash[
          *fields.map {|name|
            [
              name,
              Daru::Vector.new([]).tap {|v| v.rename name }
            ]
          }.flatten
        ]
        Daru::DataFrame.new([], order: fields).tap do |df|
          enum.each do |item|
            df.add_row(yield(item))
          end
        end
      end
    end
  end
end
