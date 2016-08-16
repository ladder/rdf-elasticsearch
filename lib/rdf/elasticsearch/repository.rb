require 'elasticsearch/persistence'
require 'rdf/elasticsearch/model/statement'

require 'pry' # TEMPORARY FOR DEBUGGING

module RDF
  module Elasticsearch
    class Repository < ::RDF::Repository
      include ::Elasticsearch::Persistence::Repository

      def initialize(options = {}, &block)
        client ::Elasticsearch::Client.new(url: options[:url] || options[:uri], log: options[:logger])

        super(options, &block)
      end

      klass RDF::Elasticsearch::Statement

      # @see RDF::Mutable#insert_statement
      def supports?(feature)
        case feature.to_sym
          when :graph_name   then true
          when :atomic_write then true
          when :validity     then @options.fetch(:with_validity, true)
          else false
        end
      end

      def clear!
        client.indices.delete index: '_all'
      end

=begin
      ##
      # @private
      # @see RDF::Enumerable#each_statement
      def each_statement(&block)
        if block_given?
          @collection.find().each do |document|
            block.call(RDF::Mongo::Conversion.statement_from_mongo(document))
          end
        end
        enum_statement
      end
      alias_method :each, :each_statement

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

      def insert_statement(statement)
        st_mongo = statement_to_mongo(statement)
        @collection.update_one(st_mongo, st_mongo, upsert: true)
      end

      # @see RDF::Mutable#delete_statement
      def delete_statement(statement)
        st_mongo = statement_to_mongo(statement)
        @collection.delete_one(st_mongo)
      end

      ##
      # @private
      # @see RDF::Durable#durable?
      def durable?; true; end

      ##
      # @private
      # @see RDF::Countable#empty?
      def empty?; @collection.count == 0; end

      ##
      # @private
      # @see RDF::Countable#count
      def count
        @collection.count
      end

      def clear_statements
        @collection.delete_many
      end

      ##
      # @private
      # @see RDF::Enumerable#has_statement?
      def has_statement?(statement)
        @collection.find(statement_to_mongo(statement)).count > 0
      end
      ##
      # @private
      # @see RDF::Enumerable#each_statement
      def each_statement(&block)
        if block_given?
          @collection.find().each do |document|
            block.call(RDF::Mongo::Conversion.statement_from_mongo(document))
          end
        end
        enum_statement
      end
      alias_method :each, :each_statement

      ##
      # @private
      # @see RDF::Enumerable#has_graph?
      def has_graph?(value)
        @collection.find(RDF::Mongo::Conversion.p_to_mongo(:graph_name, value)).count > 0
      end

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