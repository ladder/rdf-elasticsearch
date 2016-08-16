#!/usr/bin/env ruby -rubygems
# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.version            = File.read('VERSION').chomp
  gem.date               = File.mtime('VERSION').strftime('%Y-%m-%d')

  gem.name               = 'rdf-elasticsearch'
  gem.homepage           = 'https://github.com/ladder/rdf-elasticsearch'
  gem.license            = 'Apache-2.0'
  gem.summary            = 'Elasticsearch-based RDF.rb Repository'
  gem.description        = 'An RDF.rb Repository implementation using ElasticSearch for persistence and full-text querying'

  gem.authors            = ['MJ Suhonos']
  gem.email              = 'mj@suhonos.ca'

  gem.platform           = Gem::Platform::RUBY
  gem.files              = %w(LICENSE VERSION README.md) + Dir.glob('lib/**/*.rb')
  gem.require_paths      = %w(lib)
  gem.extensions         = %w()
  gem.test_files         = Dir.glob('spec/*.spec')
  gem.has_rdoc           = false

  gem.required_ruby_version      = '>= 2.3.0'
  gem.requirements               = []

  gem.add_runtime_dependency 'rdf',                       '~> 2.0'
  gem.add_runtime_dependency 'elasticsearch-persistence', '~> 0.1'

  gem.add_development_dependency 'pry'
  gem.add_development_dependency 'rdf-spec',        '~> 2.0'
  gem.add_development_dependency 'rspec',           '~> 3.4'
  gem.add_development_dependency 'rspec-its',       '~> 1.2'
  gem.add_development_dependency 'yard',            '~> 0.8'

  gem.post_install_message       = nil
end