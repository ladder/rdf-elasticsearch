require 'pry'

module RDF
  module Elasticsearch
    class Conversion
      ##
      # Translate an RDF::Value type to BSON key/value pairs.
      #
      # @param [:subject, :predicate, :object, :graph_name] position
      #   Position within statement.
      # @param [RDF::Value, Symbol] entity
      #   URI, BNode or Literal.
      # @return [Hash] BSON representation of the statement
      def self.entity_to_mongo(position, entity)
        pos = position.to_s.chr
        type = "#{pos}t".to_sym

        case entity
        when RDF::URI
          { pos => entity.to_s, type => :uri }
        when RDF::Node
          { pos => entity.id.to_s, type => :node }
        when RDF::Literal
          if entity.has_language?
            extra = "#{pos}l".to_sym
            { pos => entity.value, type => :lang, extra => entity.language.to_s }
          elsif entity.has_datatype?
            extra = "#{pos}l".to_sym
            { pos => entity.value, type => :type, extra => entity.datatype.to_s }
          else
            { pos => entity.value }
          end
        else
          {}
        end
      end

      ##
      # Translate an BSON positional reference to an RDF Value.
      #
      # @return [RDF::Value]
      def self.entity_from_mongo(value, type = :uri, extra = nil)
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
      # Create BSON for a statement representation. Note that if the statement has no graph name,
      # a value of `false` will be used to indicate the default context
      #
      # @param [RDF::Statement] statement
      # @return [Hash] Generated BSON representation of statement.
      def self.statement_from_mongo(document)
        RDF::Statement.new(
          subject:    RDF::Elasticsearch::Conversion.entity_from_mongo(document['s'], document['st'].to_sym, nil),
          predicate:  RDF::Elasticsearch::Conversion.entity_from_mongo(document['p'], :uri, nil),
          object:     RDF::Elasticsearch::Conversion.entity_from_mongo(document['o'], document['ot'], document['ol']),
          graph_name: RDF::Elasticsearch::Conversion.entity_from_mongo(document['g'], document['gt'].to_sym, nil))
      end

      ##
      # Creates a BSON representation of the statement.
      # @return [Hash]
      def self.statement_to_mongo(statement)
        h = statement.to_h.inject({}) do |hash, (position, entity)|
          hash.merge(RDF::Elasticsearch::Conversion.entity_to_mongo(position, entity))
        end
        h[:gt] ||= :default # Indicate statement is in the default graph
        h.delete(:pt) # Predicate is always a RDF::URI
        h
      end
    end
  end
end