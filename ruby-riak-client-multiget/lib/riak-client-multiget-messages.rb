require 'riak'
require 'beefcake'

Riak::Client::BeefcakeProtobuffsBackend.class_eval do
  class RpbMultiGetReq
    include Beefcake::Message
    required :bucket,        :bytes,  1
    repeated :keys,          :bytes,  2
    repeated :filter_fields, :bytes,  3
    optional :timeout,       :unit32, 4
    optional :stream,        :bool,   5, :default => false
  end

  class RpbMultiGetKVPair
    include Beefcake::Message
    required :key,   :bytes, 1
    optional :value, :bytes, 2
  end

  class RpbMultiGetResp
    include Beefcake::Message

    module RpbMultiGetStatus
      OK = 1;
      TIMEOUT = 2;
    end

    repeated :results, RpbMultiGetKVPair, 1, :default => []
    optional :done,    RpbMultiGetStatus, 2
  end
end

