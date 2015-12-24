require 'daru/td/iruby/display'
require 'daru/td/result_proxy'

require 'cgi/util'
require 'delegate'
require 'erb'
require 'strscan'

module Daru
  module TD
    class QueryEngine
      class JobWrapper < SimpleDelegator
        attr_accessor :issued_at

        class TimeoutError < StandardError; end

        def wait(timeout=nil, wait_interval=2)
          started_at = Time.now
          until finished?
            if !timeout || ((Time.now - started_at).abs > timeout && wait_interval <= timeout)
              sleep wait_interval
              yield self if block_given?
            else
              raise TimeoutError, "timeout"
            end
            update_progress!
          end
        end
      end

      def initialize(connection, database, params={}, header:false, show_progress:false, clear_progress:false)
        @connection = connection
        @database = database
        @params = params
        @header = header
        if iruby_notebook?
          # Enable progress for IRuby notebook
          @show_progress = show_progress
          @clear_progress = clear_progress
        else
          @show_progress = false
          @clear_progress = false
        end
      end

      attr_reader :connection, :show_progress, :clear_progress

      def type
        @params[:type]
      end

      def create_header(name)
        return '' unless @header
        return "-- #{@header}\n" if String === @header
        "-- #{name}\n"
      end

      def execute(query, **kwargs)
        params = @params.dup
        params.update(kwargs)

        # Issue query
        issued_at = Time.now.utc.round
        result_url = params.delete(:result_url)
        priority = params.delete(:priority)
        retry_limit = params.delete(:retry_limit)
        job = JobWrapper.new(connection.client.query(@database, query, result_url, priority, retry_limit, params))
        job.issued_at = issued_at

        get_result(job, wait: true)
      rescue Interrupt
        job.kill()
        raise
      end

      def wait_callback(job, cursize=nil)
        display_progress(job, cursize)
      end

      def job_finished?(job)
        job.update_progress!
        job.finished?
      end

      def get_result(job, wait: true)
        if wait
          job.wait(nil, 2, &method(:wait_callback))
        end

        # status check
        unless job.success?
          if job.debug && job.debug['stderr']
            #logger.error(job.debug['stderr'])
            $stderr.puts job.debug['stderr']
          end
          raise "job #{job.job_id} #{job.status}"
        end

        ResultProxy.new(self, job)
      end

      private

      def display_progress(job, cursize=nil)
        return unless show_progress
        if show_progress.is_a?(Integer) && job.issued_at
          return if Time.now.getutc < job.issued_at + show_progress
        end

        IRuby::Display.clear_output(true)
        html = render_progress_html_erb(binding)
        IRuby.display(IRuby.html(html))
      end

      def iruby_notebook?
        defined?(IRuby) && !$stdout.tty?
      end

      def render_progress_html_erb(given_binding)
        template = <<-'END_ERB'
<div style="border-style: dashed; border-width: 1px;">
  <%=html_text("issued at #{job.issued_at.iso8601}") %>
  URL: <a href="<%=job.url %>" target="_blank"><%=job.url %></a><br>
  <% if job.type == :presto %>
  <%   if job.debug && job.debug['cmdout'] %>
  <%=    html_presto_output(job.debug['cmdout']) %>
  <%   end %>
  <% end %>
  <% if job.result_size %>
  Result size: <%=escape_html(job.result_size) %> bytes<br>
  <% end %>
  <% if cursize %>
  Download: <%=escape_html(cursize) %> / <%=escape_html(job.result_size) %> bytes
  (<%=escape_html('%.2f' % [cursize * 100.0 / job.result_size]) %>%)<br>
  <%   if cursize >= job.result_size %>
  downloaded at <%=escape_html(Time.now.getutc.round.iso8601) %>
  <%   end %>
  <% end %>
</div>
        END_ERB
        erb = ERB.new(template)
        erb.filename = 'render_progress_html_erb'
        erb.result(given_binding)
      end

      def html_presto_output(cmdout)
        template = <<-'END_PRESTO_OUTPUT'
<% # started at %>
<% cmdout.scan(/started at.*$/) do |text| %>
<%=  html_text(text) %>
<% end %>
<% # warning %>
<pre style="color: #c44;">
<% cmdout.scan(/^\*{2} .*$/) do |text| %>
<%=  escape_html(text) %>
<% end %>
</pre>
<% # progress %>
<% progress = cmdout.scan(/\n\d{4}-\d{2}-\d{2}.*(?:\n +.*)+/).last %>
<% if progress %>
<pre><%=escape_html(progress) %></pre>
<% end %>
<% # rows %>
<% cmdout.scan(/^\d+ rows/) do |text| %>
<%=  escape_html(text) %><br />
<% end %>
<% # finished at %>
<% cmdout.scan(/finished at.*$/) do |text| %>
<%=  html_text(text) %>
<% end %>
        END_PRESTO_OUTPUT
        erb = ERB.new(template)
        erb.filename = 'html_presto_output'
        erb.result(binding)
      end

      def html_text(text)
        %Q[<div style="color: #888;"># #{escape_html(text)}</div>]
      end

      def escape_html(text)
        CGI.escape_html(text.to_s)
      end
    end
  end
end
