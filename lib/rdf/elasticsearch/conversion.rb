require 'pry'

module RDF
  module Elasticsearch
    class Conversion
      ##
      # Create BSON for a statement representation. Note that if the statement has no graph name,
      # a value of `false` will be used to indicate the default context
      #
      # @param [RDF::Statement] statement
      # @return [Hash] Generated BSON representation of statement.
      def self.statement_from_mongo(document)
        RDF::Statement.new(
          subject:    self.from_mongo(document['s'], document['st'], nil),
          predicate:  self.from_mongo(document['p'], :uri, nil),
          object:     self.from_mongo(document['o'], document['ot'], document['ol']),
          graph_name: self.from_mongo(document['g'], document['gt'], nil))
      end

      ##
      # Creates a BSON representation of the statement.
      # @return [Hash]
      def self.statement_to_mongo(statement)
        h = Hash.new

        statement.to_h.each do |position, entity|
          value = self.entity_to_mongo(entity)
          h["#{position}_#{value.flatten.first}"] = value.flatten.last unless value.empty?
        end

        h
      end
      
      ##
      # Translate an BSON positional reference to an RDF Value.
      #
      # @return [RDF::Value]
      def self.from_mongo(value, type = :uri, extra = nil)
        case type
        when :uri
          RDF::URI.intern(value)
        when :node
          RDF::Node.intern(value)
        when :lang
          RDF::Literal.new(value, language: extra.to_sym)
        when :type
          RDF::Literal.new(value, datatype: RDF::URI.intern(extra))
        when :default
          nil # The default context returns as nil, although it's queried as false.
        else
          RDF::Literal.new(value)
        end
      end

      ##
      # Translate an RDF::Value type to BSON key/value pairs.
      #
      # @param [RDF::Value, Symbol] entity
      #   URI, BNode or Literal.
      # @return [Hash] BSON representation of the statement
      def self.entity_to_mongo(entity)
        case entity
        when RDF::URI
          { uri: entity.to_s }
        when RDF::Node
          { node: entity.id.to_s }
        when RDF::Literal
          if entity.has_language?
            # (literal) 2-character language codes eg. :en, :fr
            { entity.language => entity.value }
          elsif entity.has_datatype?
            # (literal) :boolean, :date, :datetime, :decimal, :double, :integer, :numeric, :time, :token
            if entity.datatype.qname
              { entity.datatype.qname.join('_') => entity.value } rescue binding.pry
            else
              # TODO: FIXME
              { literal: entity.value }
            end
          else
            { literal: entity.value }
          end
        else
          {}
        end

      end

=begin
      attribute :subject,    String, # RDF::Resource (RDF::URI or RDF::Node)
                mapping: { index: 'not_analyzed' }
      attribute :predicate,  String, # RDF::URI
                mapping: { index: 'not_analyzed' }
      attribute :object,     String, # RDF::Value (RDF::URI, RDF::Node or RDF::Literal)
                mapping: { index: 'not_analyzed' }
      attribute :graph_name, String, # RDF::Resource (RDF::URI or RDF::Node)
                mapping: { index: 'not_analyzed' }

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
=end
    end
  end
end