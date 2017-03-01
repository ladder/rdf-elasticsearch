require 'pry'

module RDF
  module Elasticsearch
    class Repository < ::RDF::Repository

      attr_reader :client
      attr_reader :index

      def initialize(options = {}, &block)
        # instantiate client
        @client = ::Elasticsearch::Client.new(options)

        # create index
        @index = options['index'] || "quadb"
        @client.indices.create index: @index unless @client.indices.exists? index: @index

        # TODO: type mappings
        # RDF:Literal Types, URI, Node

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

      def insert_statement(statement)
        @client.index index: @index,
                      type: RDF::Elasticsearch::Conversion.entity_to_mongo(statement.object),
                      body: statement_to_mongo(statement)
      end

      # @see RDF::Mutable#delete_statement
      def delete_statement(statement)
binding.pry
#        st_mongo = statement_to_mongo(statement)
#        @collection.delete_one(st_mongo)
      end

      ##
      # @private
      # @see RDF::Durable#durable?
      def durable?; true; end

      ##
      # @private
      # @see RDF::Countable#empty?
      def empty?; count == 0; end

      ##
      # @private
      # @see RDF::Countable#count
      def count
        response = @client.count index: @index
        response['count']
      end

      def clear_statements
        # destroys and re-creates the index
        @client.indices.delete index: @index
        @client.indices.create index: @index
      end

      ##
      # @private
      # @see RDF::Enumerable#has_statement?
      def has_statement?(statement)
        q = statement_to_query(statement)
binding.pry
#        @collection.find(statement_to_mongo(statement)).count > 0
      end
      
      ##
      # @private
      # @see RDF::Enumerable#each_statement
      def each_statement(&block)
        if block_given?
          # Use scroll search syntax - changed in 5.x
          response = @client.search index: '_all', size: 1000, scroll: '1m', body: {sort: ['_doc']}

          # Call `scroll` until results are empty
          while response = @client.scroll(scroll_id: response['_scroll_id'], scroll: '1m') and not response['hits']['hits'].empty? do
            response['hits']['hits'].each do |hit|
              # NB: do we need hit['_type'] ?
              block.call(RDF::Elasticsearch::Conversion.statement_from_mongo(hit['_source']))
            end
          end
        
          @client.clear_scroll scroll_id: response['_scroll_id']
        end

        enum_statement
      end
      alias_method :each, :each_statement

      private

        def enumerator! # @private
          require 'enumerator' unless defined?(::Enumerable)
          @@enumerator_klass = defined?(::Enumerable::Enumerator) ? ::Enumerable::Enumerator : ::Enumerator
        end

        def statement_to_mongo(statement)
          raise ArgumentError, "Statement #{statement.inspect} is incomplete" if statement.incomplete?
          RDF::Elasticsearch::Conversion.statement_to_mongo(statement)
        end
        
        def statement_to_query(statement)
          # TODO: build an elasticsearch-api query to use with @client.search
          st_mongo = statement_to_mongo(statement)
binding.pry
        end

=begin
      def delete_statement(statement)
        @client.delete_by_query index: @index
      end

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
binding.pry
#        @collection.find(RDF::Elasticsearch::Conversion.p_to_mongo(:graph_name, value)).count > 0
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

        @collection.find(RDF::Elasticsearch::Conversion.p_to_mongo(:graph_name, value)).count > 0
      end

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

      ##
      # @private
      # @see RDF::Queryable#query_pattern
      # @see RDF::Query::Pattern
      def query_pattern(pattern, options = {}, &block)
        return enum_for(:query_pattern, pattern, options) unless block_given?

        # A pattern graph_name of `false` is used to indicate the default graph
        pm = RDF::Elasticsearch::Conversion.pattern_to_mongo(pattern)

        @collection.find(pm).each do |document|
          block.call(RDF::Elasticsearch::Conversion.statement_from_mongo(document))
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
=end
    end
  end
end