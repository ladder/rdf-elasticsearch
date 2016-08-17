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
#          when :atomic_write then true
          when :validity     then @options.fetch(:with_validity, true)
          else false
        end
      end

      def clear_statements
        # FIXME: be specfic about indexes
        client.indices.delete index: '_all' unless self.empty?

        klass.create_index! # ensure index exists
      end
      alias_method :clear!, :clear_statements

      ##
      # @private
      # @see RDF::Enumerable#each_statement
      def each_statement(&block)
        klass.find_each { |doc| block.call(doc.to_rdf) } if block_given?
        enum_statement
      end
      alias_method :each, :each_statement

      def insert_statement(statement)
        klass.create(statement_to_document(statement))
      end

      def delete_statement(statement)
        # NB: this does (at least) 2 requests: 1 to find IDs, and 1 to delete each matched document
        klass.find_each(statement_to_query(statement)) { |s| s.delete }
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
binding.pry

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

=begin
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