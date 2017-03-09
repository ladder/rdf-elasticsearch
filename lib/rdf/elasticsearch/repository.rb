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
        @refresh = options['refresh'] || options[:refresh]

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
        # TODO: this is really heavy; look into upsert/update
        return if self.has_statement? statement # don't write existing statements twice

        hash = statement_to_hash(statement)

        @client.index index: @index, type: hash.delete(:type), body: hash
        @client.indices.refresh index: @index if @refresh
      end

      # @see RDF::Mutable#delete_statement
      def delete_statement(statement)
        hash = statement_to_hash(statement)

        @client.delete_by_query index: @index, type: hash.delete(:type), body: hash_to_query(hash).to_hash, conflicts: :proceed
        @client.indices.refresh index: @index if @refresh
      end

      def clear_statements
        @client.delete_by_query index: @index, body: { }, conflicts: :proceed
        @client.indices.refresh index: @index if @refresh
      end

      ##
      # @private
      # @see RDF::Enumerable#has_statement?
      def has_statement?(statement)
        hash = statement_to_hash(statement)

        results = @client.count index: @index, type: hash.delete(:type), body: hash_to_query(hash).to_hash
        results['count'] > 0
      end

      ##
      # @private
      # @see RDF::Enumerable#has_graph?
      def has_graph?(value)
        results = @client.count index: @index, body: hash_to_query({ g: value.to_s }).to_hash
        results['count'] > 0
      end

      ##
      # @private
      # @see RDF::Enumerable#each_statement
      def each_statement(&block)
        iterate_block({}, &block) if block_given?
        enum_statement
      end
      alias_method :each, :each_statement

=begin
      def apply_changeset(changeset)
        ops = []
        changeset.deletes.each do |d|
          hash = statement_to_hash(d)
          ops << { delete_one: { filter: hash } }
        end

        changeset.inserts.each do |i|
          hash = statement_to_hash(i)
          ops << { update_one: { filter: hash, update: hash, upsert: true } }
        end

        # Only use an ordered write if we have both deletes and inserts
        ordered = ! (changeset.inserts.empty? or changeset.deletes.empty?)
        @collection.bulk_write(ops, ordered: ordered)
      end
=end
      protected

      def iterate_block(query_hash, &block)
        # Use scroll search syntax - changed in 5.x
        response = @client.search index: @index, body: query_hash, size: 1000, scroll: '1m'

        # Call `scroll` until results are empty
        until response['hits']['hits'].empty? do
          response['hits']['hits'].each do |hit|
            block.call(RDF::Elasticsearch::Conversion.statement_from_es(hit['_type'], hit['_source']))
          end
          response = @client.scroll(scroll_id: response['_scroll_id'], scroll: '1m')
        end

        @client.clear_scroll scroll_id: response['_scroll_id']
      end

      ######### RDF::Queryable #########
=begin
      ##
      # @private
      # @see RDF::Queryable#query_pattern
      # @see RDF::Query::Pattern
      def query_pattern(pattern, options = {}, &block)
        return enum_for(:query_pattern, pattern, options) unless block_given?

        # A pattern graph_name of `false` is used to indicate the default graph
        h = pattern.to_h.inject({}) do |hash, (position, entity)|
          hash.merge(p_to_mongo(position, entity))
        end
        h.merge!(gt: :default) if pattern.graph_name == false
        h.delete(:pt) # Predicate is always a RDF::URI
        st_query = hash_to_query(h)

        iterate_block(st_query.to_hash, &block)
        enum_statement
      end
      
      # @param [:subject, :predicate, :object, :graph_name] position
      #   Position within statement.
      # @param [RDF::Value, Symbol, false, nil] entity
      #   Variable or Symbol to indicate a pattern for a named graph,
      #   or `false` to indicate the default graph.
      #   A value of `nil` indicates a pattern that matches any value.
      # @return [Hash] BSON representation of the query pattern
      def p_to_mongo(position, pattern)
        pos = position.to_s.chr
        type = "#{pos}t".to_sym

        case pattern
        when RDF::Query::Variable, Symbol
          # Returns anything other than the default context
          { type => {"$ne" => :default} }
        when false
          # Used for the default context
          { type => :default}
        else
          return RDF::Elasticsearch::Conversion.entity_to_mongo(position, pattern)
        end
      end
=end
      def hash_to_query(hash)
        search do
          query do
            constant_score do
              filter do
                bool do
                  hash.each do |field, value|
                    # {"$ne" => :default}
                    if value.is_a? Hash #&& "$ne" == value.keys.first
binding.pry
                      must_not do
                        term field => value.values.first
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

      ####################################

      private

        def enumerator! # @private
          require 'enumerator' unless defined?(::Enumerable)
          @@enumerator_klass = defined?(::Enumerable::Enumerator) ? ::Enumerable::Enumerator : ::Enumerator
        end

        def statement_to_hash(statement)
          raise ArgumentError, "Statement #{statement.inspect} is incomplete" if statement.incomplete?
          RDF::Elasticsearch::Conversion.statement_to_es(statement)
        end        
    end
  end
end