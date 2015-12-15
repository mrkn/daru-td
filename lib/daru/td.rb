require 'daru/td/connection'
require 'daru/td/version'

module Daru
  module TD
    def self.connect(apikey=nil, endpoint=nil, **kwargs)
      Connection.new(apikey, endpoint, **kwargs)
    end
  end
end
