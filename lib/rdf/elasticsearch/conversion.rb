module RDF
  module Elasticsearch
    class Conversion
      include ::Elasticsearch::DSL

      def self.statement_to_id(statement)
        XXhash.xxh64(statement.to_s)
      end

      def self.statement_to_es(statement)
        h = Hash.new

        # Subject: RDF::Node or RDF::URI
        h[:s] = serialize_resource(statement.subject)

        # Predicate: RDF::URI
        # h[:p] = statement.predicate.pname.to_s
        h[:p] = statement.predicate.to_s

        # Object: will return a hash with :o and :type
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
        # p = RDF::Vocabulary.expand_pname(source["p"])
        p = RDF::URI.intern(source["p"])

        # Object
        o = deserialize_object(type, source)

        # Graph Name: RDF::Node or RDF::URI
        if source["g"]
          g = deserialize_resource(source["g"])
        end

        RDF::Statement.new(s, p, o, graph_name: g)
      end

      #
      # Serialization-related
      #

      def self.serialize_resource(resource)
        resource.is_a?(RDF::Node) ? resource.id.to_s : resource.to_s
      end

      def self.deserialize_resource(value)
        value.match(RDF::URI::IRI) ? RDF::URI.intern(value) : RDF::Node.intern(value)
      end

      def self.serialize_object(object)
        case object
        when RDF::URI
          { type: :uri, o: object.to_s }
        when RDF::Node
          { type: :node, o: object.id.to_s }
        when RDF::Literal
          if object.has_language?
            { type: "lang_#{object.language}", o: object.value }
          elsif object.has_datatype?
            # for built-in RDF::Vocabulary types, use pname eg. "xsd:boolean" -> "boolean"
            if RDF::Vocabulary.find object.datatype
              { type: object.datatype.qname.last, o: object.value }
            else
              # custom datatypes eg. with URI pnames
              { type: :typed, o: object.value, datatype: object.datatype.to_s }
            end
          else
            { type: :literal, o: object.value }
          end
        end
      end

      def self.deserialize_object(type, source)
        value = source['o']
        case type
        when 'uri'
          RDF::URI.intern(value)
        when 'node'
          RDF::Node.intern(value)
        when 'literal'
          RDF::Literal.new(value)
        when /^lang_(.+)/
          RDF::Literal.new(value, language: $1.to_sym)
        when 'typed'
          RDF::Literal.new(value, datatype: source['datatype'])
        else # built-in RDF::Literal types, eg. "xsd:boolean"
          RDF::Literal.new(value, datatype: RDF::Vocabulary.expand_pname("xsd:#{type}"))
        end
      end

      #
      # Query-related
      #

      def self.pattern_to_query(pattern)
        pat = pattern.to_h
        h = Hash.new

        case pat[:subject]
          when nil
          when RDF::Query::Variable
          else h[:s] = serialize_resource(pat[:subject])
        end

        case pat[:predicate]
          when nil
          when RDF::Query::Variable
          else h[:p] = pat[:predicate].to_s
        end

        case pat[:object]
          when nil
          when RDF::Query::Variable
          else
            serialized = serialize_object(pat[:object])
            serialized.delete :type
            h.merge! serialized
        end

        case pat[:graph_name]
          when nil
          when RDF::Query::Variable then h[:g] = :exists
          when false then h[:g] = :missing
          else h[:g] = serialize_resource(pat[:graph_name])
        end

        hash_to_query(h.compact)
      end

      def self.hash_to_query(hash)
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