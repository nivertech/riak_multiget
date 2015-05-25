$LOAD_PATH.unshift File.dirname(__FILE__)

require 'riak'
require 'riak'
require 'riak-client-multiget'

# Hack for riak instances started by `./start.sh`
Riak::Client::ProtobuffsBackend.class_eval do
  def get_server_version
    "1.4.14"
  end
end

def time_method(method=nil, *args)
  beginning_time = Time.now
  if block_given?
    yield
  else
    self.send(method, args)
  end
  end_time = Time.now
  puts "Time elapsed #{(end_time - beginning_time)*1000} milliseconds"
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

keys = (1..5).to_a.map(&:to_s)
times = 1000

puts "Map Reduce"

erl_map = Riak::MapReduce::Phase.new(type: "map", language: "erlang", function: ["riak_kv_mapreduce", "map_object_value"], arg: "filter_notfound", keep: true)
q = keys.inject(Riak::MapReduce.new(client)) { |q, i| q.add("foo", i.to_s) }
q.add("foo", "12")
q.query << erl_map
print q.run
puts
time_method do
  (1..times).each { |_| q.run.length }
end

puts "Multi Get"

time_method do
  (1..times).each { |_| client.multi_get("foo", keys).results.length }
end

