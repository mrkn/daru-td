require 'iruby/display'

module IRuby
  module Display
    unless self.respond_to?(:clear_output)
      def self.clear_output(wait=false)
        IRuby::Kernel.instance.session.send(:publish, :clear_output, {wait: wait})
      end
    end
  end
end
