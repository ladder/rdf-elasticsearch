require 'pry'

module RDF
  module Elasticsearch
    class Repository < ::RDF::Repository
      include ::Elasticsearch::DSL

      attr_reader :client
      attr_reader :index

      def initialize(options = {}, &block)
        # instantiate client
        @client = ::Elasticsearch::Client.new(options)

        # create index
        @index = options['index'] || "quadb"
        @client.indices.create index: @index unless @client.indices.exists? index: @index

        # set mapping definitions
        RDF::Elasticsearch::Mappings.ensure_mappings(self)

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
        st_mongo = statement_to_mongo(statement)

        @client.index index: @index,
                      type: st_mongo[:ot] || :literal,
                      body: st_mongo
      end

      # @see RDF::Mutable#delete_statement
      def delete_statement(statement)
        st_query = statement_to_query(statement)
        @client.delete_by_query index: @index, body: st_query.to_hash, conflicts: :proceed
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
        @client.delete_by_query index: @index, body: { }, conflicts: :proceed
      end

      ##
      # @private
      # @see RDF::Enumerable#has_statement?
      def has_statement?(statement)
        st_query = statement_to_query(statement)
        results = @client.count index: @index, body: st_query.to_hash
        results['count'] > 0
      end

      ##
      # @private
      # @see RDF::Enumerable#has_graph?
      def has_graph?(value)
        # TODO: would be good to re-use #statement_to_query if possible
        q = search do
          query do
            constant_score do
              filter do
                term g: value.to_s
              end
            end
          end
        end
        
        results = @client.count index: @index, body: q.to_hash
        results['count'] > 0
      end

      ##
      # @private
      # @see RDF::Enumerable#each_statement
      def each_statement(&block)
        if block_given?

          # Use scroll search syntax - changed in 5.x
          response = @client.search index: @index, body: { }, size: 1000, scroll: '1m'

          # Call `scroll` until results are empty
          until response['hits']['hits'].empty? do
            response['hits']['hits'].each do |hit|
              block.call(RDF::Elasticsearch::Conversion.statement_from_mongo(hit['_source']))
            end
            response = @client.scroll(scroll_id: response['_scroll_id'], scroll: '1m')
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
          st_mongo = statement_to_mongo(statement)

          search do
            query do
              constant_score do
                filter do
                  bool do
                    
                    st_mongo.each do |field, value|
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

=begin
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