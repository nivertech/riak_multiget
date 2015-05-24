$LOAD_PATH.unshift File.dirname(__FILE__)

require 'riak'
require 'lib/monkey_patches'

Riak::Client::ProtobuffsBackend.class_eval do
  def get_server_version
    "1.4.14"
  end
end

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

print client.multi_get("foo", %w{1 2}).results
puts
print client.multi_get("foo", %w{10 9}, {:filter_fields => [:a, "b"]}).results
puts
client.multi_get("foo", %w{1 12 10 5}, {}) do |res|
  puts "#{res.done == RpbMultiGetResp::RpbMultiGetStatus::OK} #{res.results.map{|r| {r.key => r.value} } }"
end

