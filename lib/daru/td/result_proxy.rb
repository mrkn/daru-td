require 'daru/td/iruby/display'

require 'open-uri'
require 'uri'
require 'msgpack'
require 'zlib'

module Daru
  module TD
    class ResultProxy
      def initialize(engine, job)
        @engine = engine
        @job = job
        @http = nil
      end

      attr_reader :engine, :job

      def status
        job.status
      end

      def size
        if !job.finished?
          job.wait()
        end
        job.result_size
      end

      def description
        if !job.finished?
          job.wait()
        end
        job.hive_result_schema
      end

      def readpartial(len=16384, outbuf="")
        @result_io ||= open_result_io()
        @result_io.readpartial(len, outbuf)
      end

      def each_record(&block)
        MessagePack::Unpacker.new(self).each(&block)
      end

      def to_dataframe(parse_dates: nil)
        fields = description.map {|c| c[0].to_sym }
        Daru::DataFrame.new([], order: fields).tap do |df|
          each_record do |record|
            df.add_row(record)
          end
          if parse_dates
            parse_date_fields(df, parse_dates)
          end
          if engine.clear_progress
            ::IRuby::Display.clear_output()
          end
        end
      end

      private

      def parse_date_fields(df, fields)
        fields.each do |name|
          parsed_values = df[name].map {|v| DateTime.parse(v) }
          df[name] = Daru::Vector.new(parsed_values, name: name)
        end
        df
      end

      def open_result_io
        Zlib::GzipReader.new(content_downloader())
      end

      def content_downloader
        ContentDownloader.new(self, job_result_uri, http_header) do |downloader|
          engine.wait_callback(job, downloader.downloaded_size)
        end
      end

      def job_result_uri
        endpoint_uri = URI.parse(self.engine.connection.endpoint)
        unless endpoint_uri.scheme
          endpoint_uri = URI.parse("https://#{endpoint_uri}")
        end
        URI.join(endpoint_uri, "v3/job/result/#{job.job_id}?format=msgpack.gz")
      end

      def http_header
        {
          'Authorization' => "TD1 #{self.engine.connection.apikey}",
          'Accept-Encoding' => 'deflate, gzip',
          'User-Agent' => "daru-td/#{Daru::TD::VERSION} (Ruby #{RUBY_VERSION})",
        }
      end

      class ContentDownloader
        def initialize(result_proxy, url, http_header, &callback)
          @callback = callback
          @url = url
          @http_header = http_header
          @downloaded_size = 0
        end

        attr_reader :downloaded_size

        def read(length=nil, outbuf="")
          if closed?
            raise IOError, "read from closed IO"
          end
          if (result = io.read(length, outbuf))
            @downloaded_size += result.bytesize
            callback
          end
          result
        end

        def readpartial(maxlen, outbuf="")
          if closed?
            raise IOError, "read from closed IO"
          end
          if (result = io.readpartial(maxlen, outbuf))
            @downloaded_size += result.bytesize
            callback
          end
          result
        end

        def closed?
          @io && @io.closed?
        end

        def close
          @io && @io.close
        end

        private

        def callback
          @callback && @callback.(self)
        end

        def io
          @io ||= open(@url, @http_header)
        end
      end
    end
  end
end
