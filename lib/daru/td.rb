require 'daru/td/connection'
require 'daru/td/query_engine'
require 'daru/td/version'

require 'active_support/hash_with_indifferent_access'
require 'cgi/core'
require 'uri'

module Daru
  module TD
    DEFAULT_ENGINE_TYPE = :presto

    # @param apikey [String]
    # @param endpoint [String]
    # @param kwargs [Hash]
    def self.connect(apikey=nil, endpoint=nil, **kwargs)
      Connection.new(apikey, endpoint, **kwargs)
    end

    # Create a handler for query engine based on a URL.
    #
    # The following environment variables are used for default connections:
    #
    # - TD_API_KEY
    # - TD_API_SERVER
    # - HTTP_PROXY
    #
    # @param uri [String] Engine descriptor in the form "type://apikey@host/database?params..."
    #   Use shorthand notation "type:database?params..." for the default connection.
    # @param conn [Connection, nil] Handler returned by connect.
    #   If not given, the default connection is used.
    # @param header [String, true, false] Prepend comment strings, in the form "-- comment",
    #   as a header of queries.  Set false to disable header.
    # @param show_progress [Float, true, false]  Number of seconds to wait before printing progress.
    #   Set false to disable progress entirely.
    # @param clear_progress [true, false] If true, clear progress when query completed.
    # @return [QueryEngine]
    def self.create_engine(uri, conn:nil, header:true, show_progress:5.0, clear_progress:true)
      uri = URI.parse(uri)
      engine_type = (uri.scheme || DEFAULT_ENGINE_TYPE).to_sym
      unless conn
        if uri.host
          apikey, host = uri.userinfo, uri.host
          conn = connect(apikey, "https://#{host}/")
        else
          conn = connect()
        end
      end
      database = uri.path || uri.opaque
      database = database[1..-1] if database.start_with?('/')
      params = {
        type: engine_type
      }
      params.update(parse_query(uri.query)) if uri.query
      return QueryEngine.new(conn, database, params,
                             header: header,
                             show_progress: show_progress,
                             clear_progress: clear_progress)
    end

    # Read Treasure Data query into a Daru's DataFrame.
    #
    # Returns a Daru::DataFrame corresponding to the result set of the query string.
    #
    # @param query [String]
    #   Query string to be executed.
    # @param engine [Daru::TD::QueryEngine]
    #   Handler returned by create_engine.
    # @param parse_dates [Array, nil]
    #   When an Array given, it has column names to parse as dates.
    # @param distributed_join [true, false]
    #   (Presto only) If true, distributed join is enabled.
    #   If false (default), broadcast join is used.
    #   See https://prestodb.io/docs/current/release/release-0.77.html
    # @params kwargs [Hash, nil]
    #   Parameters to pass to execute method.
    #   Available parameters:
    #   - result_url [String] is result output URL.
    #   - priority [Integer, String] is job's priority (e.g. "NORMAL", "HIGH", etc.)
    #   - retry_limit [Integer] is retry limit.
    # @return [Daru::DataFrame]
    def self.read_td_query(query, engine, **kwargs)
      distributed_join = kwargs.delete(:distributed_join)
      parse_dates = kwargs.delete(:parse_dates)
      header = engine.create_header('read_td_query')
      if engine.type == :presto && distributed_join
        header += "-- set session distributed_join = #{!!distributed_join}\n"
      end
      result = engine.execute(header + query, **kwargs)
      result.to_dataframe(parse_dates: parse_dates)
    end

    # Read Treasure Data job result int a Daru's DataFrame.
    #
    # Returns a DataFrame corresponding to the result set of the job.
    # This method waits for job completion if the specified job is still running.
    #
    # @param job_id [Integer] Job ID.
    # @param engine [Daru::TD::QueryEngine]
    #   Handler returned by create_engine.
    # @param parse_dates [Array, nil]
    #   When an Array given, it has column names to parse as dates.
    # @return [Daru::DataFrame]
    def self.read_td_job(job_id, engine, **kwargs)
      parse_dates = kwargs.delete(:parse_dates)
      job = QueryEngine::JobWrapper.new(engine.connection.client.job(job_id))
      result = engine.get_result(job, wait: true)
      result.to_dataframe(parse_dates: parse_dates)
    end

    def self.parse_query(query_string)
      CGI.parse(query_string).tap do |hash|
        hash.keys.each do |key|
          hash[key.to_sym] = hash.delete(key)
        end
      end
    end
    private_class_method :parse_query
  end
end
