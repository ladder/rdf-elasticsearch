require 'rdf'
require 'enumerator'
require 'elasticsearch'
require 'elasticsearch/dsl'

require 'rdf/elasticsearch/conversion'
require 'rdf/elasticsearch/mappings'
require 'rdf/elasticsearch/repository'

module RDF
  module Elasticsearch
    autoload :VERSION, "rdf/elasticsearch/version"
  end
end