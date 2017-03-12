require 'pry'

module RDF
  module Elasticsearch
    class Conversion
      include ::Elasticsearch::DSL

      def self.statement_to_es(statement)
        h = Hash.new

        # Subject: RDF::Node or RDF::URI
        h[:s] = serialize_resource(statement.subject)

        # Predicate: RDF::URI
        h[:p] = statement.predicate.to_s

        # Object
        h.merge!(serialize_object(statement.object))

        # Graph Name: RDF::Node or RDF::URI
        if statement.has_graph?
          h[:g] = serialize_resource(statement.graph_name)
        end

        h
      end

      def self.statement_from_es(type, source)
        # Subject: RDF::Node or RDF::URI
        s = deserialize_resource(source["s"])

        # Predicate: RDF::URI
        p = RDF::URI.intern(source["p"])

        # Object
        o = deserialize_object(type, source[type])

        # Graph Name: RDF::Node or RDF::URI
        if source["g"]
          g = deserialize_resource(source["g"])
        end

        RDF::Statement.new(s, p, o, graph_name: g)
      end

      def self.serialize_resource(resource)
        resource.is_a?(RDF::Node) ? resource.id.to_s : resource.to_s
      end

      def self.deserialize_resource(value)
        value.match(RDF::URI::IRI) ? RDF::URI.intern(value) : RDF::Node.intern(value)
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
            # FIXME: how to handle URI pnames?
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
          # TODO: fold this into when block
          if type =~ /lang_(.+)/
            RDF::Literal.new(value, language: $1.to_sym)
          else
            RDF::Literal.new(value, datatype: RDF::Vocabulary.expand_pname(type))
          end
        end
      end

      def self.pattern_to_query(pattern)
        # {:subject=>nil, :predicate=>nil, :object=>nil, :graph_name=>false}
        pat = pattern.to_h
        h = Hash.new

        if pat[:subject].nil? # NOP
        elsif pat[:subject].is_a?(RDF::Query::Variable)
          # h[:s] = :exists
        else h[:s] = self.serialize_resource(pat[:subject])
        end

        if pat[:predicate].nil? # NOP
        elsif pat[:predicate].is_a?(RDF::Query::Variable)
          # h[:p] = :exists
        else h[:p] = pat[:predicate].to_s
        end

        if pat[:object].nil? # NOP
        elsif pat[:object].is_a?(RDF::Query::Variable)
          # FIXME: this fails when looking for typed objects
          # ie. existence has to check on the :type field (eg. :literal)
          # h[:o] = :exists
        else
          serialized = self.serialize_object(pat[:object])
          # TODO: query on :o field instead of typed (?)
          serialized.delete :type
          h.merge! serialized
        end

        if pat[:graph_name].nil? # NOP
        elsif pat[:graph_name].is_a?(RDF::Query::Variable)
          h[:g] = :exists
        elsif false == pat[:graph_name]
          h[:g] = :missing
        else
          h[:g] = self.serialize_resource(pat[:graph_name])
        end

        hash_to_query(h.compact)
      end

      def self.hash_to_query(hash)
        # Explicitly default to no graph name (default graph)
#binding.pry if hash[:g].nil?
#        hash[:g] = :missing if hash[:g].nil?

        # FIXME: this (self.new) is a bit weird
        self.new.search do
          query do
            constant_score do
              filter do
                if hash.empty?
                  match_all
                else
                  bool do
                    hash.each do |field, value|
                      case value
                      when :exists
                        must do
                          exists field: field
                        end
                      when :missing
                        must_not do
                          exists field: field
                        end
                      else
                        must do
                          term field => value
                        end
                      end

                    end
                  end

                end
              end
            end
          end
        end
      end
    end
  end
end