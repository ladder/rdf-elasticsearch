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

        RDF::Statement.new(s, p, o, graph_name: g)
      end

      def self.serialize_object(object)
        case object
        when RDF::URI
          { type: :uri, uri: object.to_s }
        when RDF::Node
          { type: :node, node: object.id.to_s }
        when RDF::Literal
          if object.has_language?
            type = "lang_#{object.language}".to_sym
            { type: type, type => object.value }

          elsif object.has_datatype?
            type = object.datatype.pname.to_sym
            { type: type, type => object.value }
          else
            { type: :literal, literal: object.value }
          end
        end
      end
      
      def self.deserialize_object(type, value)
        case type
        when 'uri'
          RDF::URI.intern(value)
        when 'node'
          RDF::Node.intern(value)
        when 'literal'
          RDF::Literal.new(value)
        else
          if type =~ /lang_(.+)/
            RDF::Literal.new(value, language: $1.to_sym)
          else
            RDF::Literal.new(value, datatype: RDF::Vocabulary.expand_pname(type))
          end
        end
      end

    end
  end
end