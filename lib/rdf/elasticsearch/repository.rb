module RDF
  module Elasticsearch
    class Repository < ::RDF::Repository
      attr_reader :client
      attr_reader :index

      def initialize(options = {}, &block)
        # instantiate client
        @client = ::Elasticsearch::Client.new(options)

        # force realtime behaviour (MUCH SLOWER)
        @refresh = options['refresh'] || options[:refresh]

        # create index
        @index = options['index'] || "quadb"

        @client.indices.create index: @index,
                               body: RDF::Elasticsearch::Mappings.index unless @client.indices.exists? index: @index

        super(options, &block)
      end

      # @see RDF::Mutable#insert_statement
      def supports?(feature)
        case feature.to_sym
          when :graph_name       then true
          when :atomic_write     then true
          # when :literal_equality then true
          when :validity         then @options.fetch(:with_validity, true)
          else false
        end
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

      def insert_statement(statement)
        hash = statement_to_hash(statement)
        @client.index index: @index,
                      type: hash.delete(:type),
                      refresh: @refresh,
                      id: RDF::Elasticsearch::Conversion.statement_to_id(statement),
                      body: hash
      end

      # @see RDF::Mutable#delete_statement
      def delete_statement(statement)
        hash = statement_to_hash(statement)
        @client.delete index: @index,
                       type: hash.delete(:type),
                       ignore: 404,
                       refresh: @refresh,
                       id: RDF::Elasticsearch::Conversion.statement_to_id(statement)
      end

      def clear_statements
        @client.delete_by_query index: @index,
                                conflicts: :proceed,
                                refresh: @refresh,
                                body: { } # match all documents
      end

      ##
      # @private
      # @see RDF::Enumerable#has_statement?
      def has_statement?(statement)
        @client.exists index: @index,
                       id: RDF::Elasticsearch::Conversion.statement_to_id(statement)
      end

      ##
      # @private
      # @see RDF::Enumerable#has_graph?
      def has_graph?(value)
        hash = { g: value.to_s } # TODO: this belongs in #hash_to_query somehow
        response = @client.count index: @index,
                                 body: RDF::Elasticsearch::Conversion.hash_to_query(hash).to_hash
        response['count'] > 0
      end

      ##
      # @private
      # @see RDF::Enumerable#each_statement
      def each_statement(&block)
        iterate_block(RDF::Elasticsearch::Conversion.hash_to_query({}).to_hash, &block) if block_given?
        enum_statement
      end
      alias_method :each, :each_statement

      def apply_changeset(changeset)
        ops = []

        changeset.deletes.each do |d|
          hash = statement_to_hash(d)
          ops << { delete: { _index: @index,
                             _type: hash.delete(:type),
                             _id: RDF::Elasticsearch::Conversion.statement_to_id(d) } }
        end

        changeset.inserts.each do |i|
          hash = statement_to_hash(i)
          ops << { index:  { _index: @index,
                             _type: hash.delete(:type),
                             _id: RDF::Elasticsearch::Conversion.statement_to_id(i),
                             data: hash } }
        end

        @client.bulk body: ops, refresh: @refresh unless ops.empty?
      end

      protected

      ##
      # @private
      # @see RDF::Queryable#query_pattern
      # @see RDF::Query::Pattern
      def query_pattern(pattern, options = {}, &block)
        return enum_for(:query_pattern, pattern, options) unless block_given?
        iterate_block(RDF::Elasticsearch::Conversion.pattern_to_query(pattern).to_hash, &block)
      end

      private

        def enumerator! # @private
          require 'enumerator' unless defined?(::Enumerable)
          @@enumerator_klass = defined?(::Enumerable::Enumerator) ? ::Enumerable::Enumerator : ::Enumerator
        end

        def statement_to_hash(statement)
          raise ArgumentError, "Statement #{statement.inspect} is incomplete" if statement.incomplete?
          RDF::Elasticsearch::Conversion.statement_to_es(statement)
        end

        def iterate_block(query_hash, &block)
          # Use scroll search syntax

          response = @client.search index: @index, size: 1000, scroll: '1m', body: query_hash

          # Call `scroll` until hits are empty
          until response['hits']['hits'].empty? do
            response['hits']['hits'].each do |hit|
              block.call(RDF::Elasticsearch::Conversion.statement_from_es(hit['_type'], hit['_source']))
            end
            response = @client.scroll(scroll_id: response['_scroll_id'], scroll: '1m')
          end

          @client.clear_scroll scroll_id: response['_scroll_id']
        end
    end
  end
end