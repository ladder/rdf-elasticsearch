require 'pry'

module RDF
  module Elasticsearch
    class Conversion

      def self.statement_to_es(statement)
        h = Hash.new

        # Subject: RDF::Node or RDF::URI
        h[:s] = statement.subject.is_a?(RDF::Node) ? statement.subject.id.to_s : statement.subject.to_s

        # Predicate: RDF::URI
        h[:p] = statement.predicate.to_s

        # Object
        h.merge!(serialize_object(statement.object))

        # Graph Name: RDF::Node or RDF::URI
        if statement.has_graph?
          h[:g] = statement.graph_name.is_a?(RDF::Node) ? statement.graph_name.id.to_s : statement.graph_name.to_s
        end
# TODO: inject "type" value into hash for index type
        h
      end
      
      def self.statement_from_es(type, source)
        
        # Subject: RDF::Node or RDF::URI
        s = source["s"].match(RDF::URI::IRI) ? RDF::URI.intern(source["s"]) : RDF::Node.intern(source["s"])
        
        # Predicate: RDF::URI
        p = RDF::URI.intern(source["p"])
        
        # Object
        o = deserialize_object(type, source[type])

        # Graph Name: RDF::Node or RDF::URI
        if source["g"]
          g = source["g"].match(RDF::URI::IRI) ? RDF::URI.intern(source["g"]) : RDF::Node.intern(source["g"])
        end

statement=        RDF::Statement.new(s, p, o, graph_name: g)
binding.pry
      end

      def self.serialize_object(object)
        case object
        when RDF::URI
          { uri: object.to_s }
        when RDF::Node
          { node: object.id.to_s }
        when RDF::Literal
          if object.has_language?
            { "lang_#{object.language}" => object.value }
          elsif object.has_datatype?
            { "lang_#{object.datatype}" => object.value }
          else
            { literal: object.value }
          end
        else
binding.pry
          {}
        end
      end
      
      def self.deserialize_object(type, value)
        case type.to_sym
        when :uri
          RDF::URI.intern(value)
        when :node
          RDF::Node.intern(value)
=begin
        when :lang
          RDF::Literal.new(value, language: extra.to_sym)
        when :type
          RDF::Literal.new(value, datatype: RDF::URI.intern(extra))
=end
        when :boolean
        when :date
        when :datetime
        when :decimal
        when :double
        when :integer
        when :numeric
        when :time
        when :token
binding.pry          
        else
binding.pry if type.match "lang_"
          RDF::Literal.new(value)
        end
      end
      
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