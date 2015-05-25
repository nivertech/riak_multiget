require 'riak'
require 'beefcake'
require 'riak-client-multiget-messages'

Riak::Client::BeefcakeProtobuffsBackend.class_eval do
  def multi_get(bucket, keys, query_options={}, &block)
    bucket = bucket.name if Riak::Bucket === bucket

    options = {:bucket => bucket, :keys => keys}
    filter_fields = query_options[:filter_fields]
    options.merge!(query_options)
    options[:filter_fields] = filter_fields.map(&:to_s) if filter_fields
    options[:stream] = block_given?

    req = RpbMultiGetReq.new(options)
    write_protobuff(:MultiGetReq, req)
    decode_multi_get_response(&block)
  end

  def decode_multi_get_response(&block)
    loop do
      header = socket.read(5)
      raise SocketError, "Unexpected EOF on PBC socket" if header.nil?
      msglen, msgcode = header.unpack("NC")
      message = socket.read(msglen-1)
      case Riak::Client::BeefcakeMessageCodes[msgcode]
      when :ErrorResp
        res = Riak::Client::BeefcakeProtobuffsBackend::RpbErrorResp.decode(message)
        raise Riak::ProtobuffsFailedRequest.new(res.errcode, res.errmsg)
      when :MultiGetResp
        res = RpbMultiGetResp.decode(message)
        if block_given?
          yield res
          return if res.done
        else
          return res
        end
      end
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
