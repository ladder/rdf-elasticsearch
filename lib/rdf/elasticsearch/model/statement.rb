require 'elasticsearch/persistence/model'

module RDF
  module Elasticsearch
    class Statement < ::RDF::Statement
      include ::Elasticsearch::Persistence::Model
    end
  end
end