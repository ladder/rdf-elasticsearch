require 'elasticsearch/persistence/model'

module RDF
  module Elasticsearch
    class Statement #< ::RDF::Statement
      include ::Elasticsearch::Persistence::Model

      attribute :subject,    String, # RDF::Resource (RDF::URI or RDF::Node)
                mapping: { index: 'not_analyzed' }
      attribute :predicate,  String, # RDF::URI
                mapping: { index: 'not_analyzed' }
      attribute :object,     String, # RDF::Value (RDF::URI, RDF::Node or RDF::Literal)
                mapping: { index: 'not_analyzed' }
      attribute :graph_name, String, # RDF::Resource (RDF::URI or RDF::Node)
                mapping: { index: 'not_analyzed' }

      # attribute :type, String # TODO: literal type information
      attribute :id,     String # TODO: use POST to let ES assign IDs

      ##
      # @return RDF::Statement
      def to_rdf
        # FIXME: this validation is wrong due to a bug in RDF.rb
        s = subject.match(RDF::URI::IRI) ? RDF::URI.intern(subject) : RDF::Node.intern(subject)

        # predicate
        p = RDF::URI.intern(predicate)

        # object
        # FIXME: literal type / language checking
        o = RDF::Literal.new(object)

        # graph name
        g = graph_name.match(RDF::URI::IRI) ? RDF::URI.intern(graph_name) : RDF::Node.intern(graph_name)
        g = nil if graph_name == ''

        RDF::Statement.new(s, p, o, graph_name: g)
      end
    end
  end
end