require 'rubygems'
require 'riak'
require 'beefcake'

$LOAD_PATH.unshift File.dirname(__FILE__)

Riak::Client::ProtobuffsBackend.class_eval do
  def get_server_version
    "1.4.14"
  end
end

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

    def to_s
      "#{key} => #{value}"
    end
  end

  class RpbMultiGetResp
    include Beefcake::Message

    module RpbMultiGetStatus
      OK = 1;
      TIMEOUT = 2;
    end

    repeated :results, RpbMultiGetKVPair, 1
    optional :done,    RpbMultiGetStatus, 2
  end

  def multi_get(bucket, keys, query_options={}, &block)
    return super unless pb_indexes?
    bucket = bucket.name if Riak::Bucket === bucket

    options = {:bucket => bucket, :keys => keys}
    options.merge!(query_options)
    options[:stream] = block_given?

    req = RpbMultiGetReq.new(options)
    write_protobuff(:MultiGetReq, req)
    if block_given?
      # TODO
      puts "Unimplemented"
    else
      decode_multi_get_response
    end
  end

  def decode_multi_get_response
    header = socket.read(5)
    raise SocketError, "Unexpected EOF on PBC socket" if header.nil?
    msglen, msgcode = header.unpack("NC")
    message = socket.read(msglen-1)
    case Riak::Client::BeefcakeMessageCodes[msgcode]
    when :ErrorResp
      res = RpbErrorResp.decode(message)
      raise Riak::ProtobuffsFailedRequest.new(res.errcode, res.errmsg)
    when :MultiGetResp
      RpbMultiGetResp.decode(message)
    end
  rescue SystemCallError, SocketError => e
    reset_socket
    raise
  end

end

Riak::Client.class_eval do
    def multi_get(bucket, keys, options = {}, &block)
      backend do |b|
        b.multi_get(bucket, keys, options, &block)
      end
    end
end

Riak::Client::BeefcakeMessageCodes::MESSAGE_TO_CODE.merge!({:MultiGetReq => 101, :MultiGetResp => 102})
original_verbosity = $VERBOSE
$VERBOSE = nil
Riak::Client::BeefcakeMessageCodes::CODE_TO_MESSAGE = Riak::Client::BeefcakeMessageCodes::MESSAGE_TO_CODE.invert
$VERBOSE = original_verbosity

client = Riak::Client.new(
  :pb_port => 8071,
  :protocol => 'pbc'
)

bucket = client.bucket "foo"
(1..10).each do |i|
  object = bucket.get_or_new(i.to_s)
  object.raw_data = { :a => i.odd?, :b => 'banana', :c => (1..i).to_a }.to_json
  object.store
end

puts client.multi_get("foo", %w{1 2 3}).results
