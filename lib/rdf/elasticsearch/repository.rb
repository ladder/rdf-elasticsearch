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
        
        # force realtime behaviour (MUCH SLOWER)
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
#puts "\n----\n"
#puts statement.to_h
        hash = statement_to_hash(statement)
#puts hash
        results = @client.count index: @index, type: hash.delete(:type), body: hash_to_query(hash).to_hash
#puts results
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
        iterate_block(hash_to_query({}).to_hash, &block) if block_given?
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

      ##
      # @private
      # @see RDF::Queryable#query_pattern
      # @see RDF::Query::Pattern
      def query_pattern(pattern, options = {}, &block)
        return enum_for(:query_pattern, pattern, options) unless block_given?
        iterate_block(pattern_to_query(pattern).to_hash, &block)
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

        def pattern_to_query(pattern)
          # {:subject=>nil, :predicate=>nil, :object=>nil, :graph_name=>false}
          pat = pattern.to_h

          h = Hash.new
        
          if pat[:subject].nil? || pat[:subject].is_a?(RDF::Query::Variable) # NOP
          else h[:s] = pat[:subject].to_s
          end

          if pat[:predicate].nil? || pat[:predicate].is_a?(RDF::Query::Variable) # NOP
          else h[:p] = pat[:predicate].to_s
          end

          if pat[:object].nil? || pat[:object].is_a?(RDF::Query::Variable) # NOP
          else
            serialized = RDF::Elasticsearch::Conversion.serialize_object(pat[:object])
            serialized.delete :type
            h.merge! serialized
          end

          if pat[:graph_name].nil? # NOP
          elsif false == pat[:graph_name]
            h[:g] = :missing
          else
            h[:g] = pat[:graph_name].is_a?(RDF::Node) ? pat[:graph_name].id.to_s : pat[:graph_name].to_s
          end

          hash_to_query(h.compact)
        end
      
        def hash_to_query(hash)
          search do
            query do
              constant_score do
                filter do
                  if hash.empty?
                    match_all
                  else
                    bool do
                      hash.each do |field, value|
                        case value
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