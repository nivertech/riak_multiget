Gem::Specification.new do |s|
  s.name        = 'riak-client-multiget'
  s.version     = '0.1.0'
  s.licenses    = ['MIT']
  s.summary     = "riak_multiget extensions for riak-client"
  s.description = "riak_multiget extensions for riak-client"
  s.authors     = ["Valery Meleshkin"]
  s.email       = 'valery.meleshkin@gmail.com'
  s.homepage    = 'http://github.com/wooga/riak_multiget'

  s.add_runtime_dependency "riak-client", "~> 1.4"

  s.files       = ["lib/riak-client-multiget.rb", "lib/riak-client-multiget-messages.rb"]
end
