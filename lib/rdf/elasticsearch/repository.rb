require 'elasticsearch'

require 'pry' # TEMPORARY FOR DEBUGGING

module RDF
  module Elasticsearch
    class Repository < ::RDF::Repository
      
      INDEXES = ['uri', 'node', 'literal']

      def initialize(options = {}, &block)
        # instantiate client
        @client = ::Elasticsearch::Client.new(options)
        
        # ensure indexes exist
        INDEXES.each do |i|
          @client.indices.create index: i unless @client.indices.exists? index: i
        end

        super(options, &block)
      end

      # @see RDF::Mutable#insert_statement
      def supports?(feature)
        case feature.to_sym
          when :graph_name   then true
#          when :atomic_write then true
          when :validity     then @options.fetch(:with_validity, true)
          else false
        end
      end

=begin
      def clear_statements
        # destroys and re-creates all the indexes
        INDEXES.each do |i|
          @client.indices.delete index: i if @client.indices.exists? index: i
          @client.indices.create index: i
        end
      end
      alias_method :clear!, :clear_statements
=end
      
      ### private methods
      
      def node_to_index(node)
        case node
          when RDF::URI
            'uri'
          when RDF::Node
            'node'
          when RDF::Literal
            'literal'
          else
            # TEMPORARY
            binding.pry
        end
      end
      
      def statement_to_hash(statement)
        JSON.parse(statement.to_h.to_json)
      end
      
      ##
      # @return RDF::Statement
      def hash_to_statement(hash)
        # FIXME: this validation is wrong due to a bug in RDF.rb
        s = hash['subject'].match(RDF::URI::IRI) ? RDF::URI.intern(hash['subject']) : RDF::Node.intern(hash['subject'])

        # predicate
        p = RDF::URI.intern(hash['predicate'])

        # object
        # FIXME: literal type / language checking
        o = RDF::Literal.new(hash['object'])

        # graph name
        if hash['graph_name'].nil?
          g = nil
        else
          g = hash['graph_name'].match(RDF::URI::IRI) ? RDF::URI.intern(hash['graph_name']) : RDF::Node.intern(hash['graph_name'])
        end

        RDF::Statement.new(s, p, o, graph_name: g)
      end

      ### ###

      ##
      # @private
      # @see RDF::Enumerable#each_statement
      def each_statement(&block)
        # Use scroll search syntax - changed in 5.x
        response = @client.search index: '_all', size: 1000, scroll: '1m', body: {sort: ['_doc']}

        # Call `scroll` until results are empty
        while response = @client.scroll(scroll_id: response['_scroll_id'], scroll: '1m') and not response['hits']['hits'].empty? do
          response['hits']['hits'].each do |hit|
            statement = hash_to_statement(hit['_source'])
            block.call(statement) if block_given?
          end
        end
        
        @client.clear_scroll scroll_id: response['_scroll_id']

        enum_statement
      end
      alias_method :each, :each_statement

      def insert_statement(statement)
        i = node_to_index(statement.object)
        # FIXME: how to handle type?
        @client.index index: i, type: i, body: statement_to_hash(statement)
      end

      def delete_statement(statement)
        i = node_to_index(statement.object)
#binding.pry
#        @client.delete_by_query index: i
      end

=begin

      ##
      # @private
      # @see RDF::Enumerable#has_statement?
      def has_statement?(statement)
        q = statement_to_query(statement)
        klass.count(q) > 0
      end

      ##
      # @private
      # @see RDF::Enumerable#has_graph?
      def has_graph?(value)
        f = {
          term: { graph_name: statement.graph_name.to_s }
        }

        q = {
          query: {
            filtered: {
              query: { match_all: {} },
              filter: f
            }
          }
        }

        @collection.find(RDF::Mongo::Conversion.p_to_mongo(:graph_name, value)).count > 0
      end

      ##
      # @private
      # @see RDF::Durable#durable?
      def durable?; true; end

      ##
      # @private
      # @see RDF::Countable#empty?
      def empty?; klass.count == 0; end

      ##
      # @private
      # @see RDF::Countable#count
      def count
        klass.count
      end


      #### TEMPORARY
      def statement_to_document(statement)
        # TODO: this will become RDF::Statement.to_document or similar
        h = statement.to_hash
        h.update(h) { |k,v| v.to_s }
        h.merge!(id: statement.object_id)
      end

      def statement_to_query(statement)
        # TODO: this will become RDF::Statement.to_query or similar
        f = {
          bool: {
            must: [
              {
                term: { subject: statement.subject.to_s }
              },
              {
                term: { predicate: statement.predicate.to_s }
              },
              {
                term: { object: statement.object.to_s }
              },
              {
                term: { graph_name: statement.graph_name.to_s }
              }
            ]
          }
        }

        q = {
          query: {
            filtered: {
              query: { match_all: {} },
              filter: f
            }
          }
        }
      end
      #### TEMPORARY

      protected

      ##
      # @private
      # @see RDF::Queryable#query_pattern
      # @see RDF::Query::Pattern
      def query_pattern(pattern, options = {}, &block)
        return enum_for(:query_pattern, pattern, options) unless block_given?

        # A pattern graph_name of `false` is used to indicate the default graph
        pm = RDF::Mongo::Conversion.pattern_to_mongo(pattern)

        @collection.find(pm).each do |document|
          block.call(RDF::Mongo::Conversion.statement_from_mongo(document))
        end
      end

      def apply_changeset(changeset)
        ops = []

        changeset.deletes.each do |d|
          st_mongo = statement_to_mongo(d)
          ops << { delete_one: { filter: st_mongo } }
        end

        changeset.inserts.each do |i|
          st_mongo = statement_to_mongo(i)
          ops << { update_one: { filter: st_mongo, update: st_mongo, upsert: true } }
        end

        # Only use an ordered write if we have both deletes and inserts
        ordered = ! (changeset.inserts.empty? or changeset.deletes.empty?)
        @collection.bulk_write(ops, ordered: ordered)
      end

      private

        def enumerator! # @private
          require 'enumerator' unless defined?(::Enumerable)
          @@enumerator_klass = defined?(::Enumerable::Enumerator) ? ::Enumerable::Enumerator : ::Enumerator
        end

        def statement_to_mongo(statement)
          raise ArgumentError, "Statement #{statement.inspect} is incomplete" if statement.incomplete?
          RDF::Mongo::Conversion.statement_to_mongo(statement)
        end
=end
    end
  end
end