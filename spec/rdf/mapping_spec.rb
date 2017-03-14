$:.unshift "."
require 'spec_helper'

require 'rdf/elasticsearch'
require 'pry'

include Elasticsearch::DSL

describe RDF::Elasticsearch::Mappings do
  before :all do
    @load_durable = lambda { RDF::Elasticsearch::Repository.new uri: "http://localhost:9200",
                                                                clean: true,
                                                                refresh: true,
                                                                log: false }
    @repository = @load_durable.call
  end

  before :each do
    @repository.clear_statements
  end

  shared_examples "an RDF::Elasticsearch::Mapping" do
    let (:statement) { RDF::Statement.new(RDF::Node.new, RDF::URI.new("urn:predicate:1"), subject) }

    before do
      # create and persist a statement
      @repository.insert_statement(statement)
puts "\n"
    end

    it 'should match default query' do
      hash = RDF::Elasticsearch::Conversion.statement_to_es(statement)
      body = RDF::Elasticsearch::Conversion.hash_to_query(hash)
puts body.to_hash
      @repository.has_statement? statement
    end

    it 'should be searchable typed' do
      hash = RDF::Elasticsearch::Conversion.serialize_object(subject)
      field = hash.delete(:type)

      body = search do
        query do
          match hash
        end
      end
puts body.to_hash
      response = @repository.client.search index: @repository.index, body: body.to_hash
      hit = response['hits']['hits'].first
      deserialized = RDF::Elasticsearch::Conversion.statement_from_es(hit['_type'], hit['_source'])

      expect(deserialized).to eq(statement)
    end

    it 'should be searchable untyped' do
      hash = RDF::Elasticsearch::Conversion.serialize_object(subject)
      value = hash[hash.delete(:type)]

      body = search do
        query do
          simple_query_string do
            query value
          end
        end
      end
puts body.to_hash
      response = @repository.client.search index: @repository.index, body: body.to_hash
      hit = response['hits']['hits'].first
      deserialized = RDF::Elasticsearch::Conversion.statement_from_es(hit['_type'], hit['_source'])

      response = @repository.client.search index: @repository.index, body: body.to_hash
      expect(deserialized).to eq(statement)
    end
  end

  context "with RDF::URI" do
    let (:subject) { RDF::URI.new("urn:object:1") }

    it_behaves_like 'an RDF::Elasticsearch::Mapping'
  end

  context "with RDF::Node" do
    let (:subject) { RDF::Node.new('object:1') }

    it_behaves_like 'an RDF::Elasticsearch::Mapping'
  end

  context "with RDF::Literal" do
    let (:subject) { RDF::Literal.new('abc') }

    it_behaves_like 'an RDF::Elasticsearch::Mapping'
  end

  #
  # Data-typed RDF::Literals
  #

  # boolean
  context "with RDF::Literal::Boolean" do
    let (:subject) { RDF::Literal.new(false, datatype: RDF::XSD.boolean) }

    it_behaves_like 'an RDF::Elasticsearch::Mapping'
  end

  # date
  context "with RDF::Literal::Date" do
    let (:subject) { RDF::Literal.new(Date.new(2010), datatype: RDF::XSD.date) }

    it_behaves_like 'an RDF::Elasticsearch::Mapping'
  end

  # datetime
  context "with RDF::Literal::DateTime" do
    let (:subject) { RDF::Literal.new(DateTime.new(2011), datatype: RDF::XSD.dateTime) }

    it_behaves_like 'an RDF::Elasticsearch::Mapping'
  end

  # decimal
  context "with RDF::Literal::Decimal" do
    let (:subject) { RDF::Literal.new(1.1, datatype: RDF::XSD.decimal) }

    it_behaves_like 'an RDF::Elasticsearch::Mapping'
  end

  # double
  context "with RDF::Literal::Double" do
    let (:subject) { RDF::Literal.new(3.1415, datatype: RDF::XSD.double) }

    it_behaves_like 'an RDF::Elasticsearch::Mapping'
  end

  # integer
  context "with RDF::Literal::Integer" do
    let (:subject) { RDF::Literal.new(1, datatype: RDF::XSD.integer) }

    it_behaves_like 'an RDF::Elasticsearch::Mapping'
  end

  # time
  context "with RDF::Literal::Time" do
    let (:subject) { RDF::Literal.new(Time.now, datatype: RDF::XSD.time) }

    it_behaves_like 'an RDF::Elasticsearch::Mapping'
  end

  # token
  context "with RDF::Literal::Token" do
    let (:subject) { RDF::Literal.new(:xyz, datatype: RDF::XSD.token) }

    it_behaves_like 'an RDF::Elasticsearch::Mapping'
  end

  #
  # Language-tagged RDF::Literals
  #

  context "with English literal" do
    let (:subject) { RDF::Literal.new('abc', language: 'en') }

    it_behaves_like 'an RDF::Elasticsearch::Mapping'
  end

  context "with Finnish literal" do
    let (:subject) { RDF::Literal.new('abc', language: 'fi') }

    it_behaves_like 'an RDF::Elasticsearch::Mapping'
  end

end
