$:.unshift "."
require 'spec_helper'

#require 'rdf/spec/literal'
require 'rdf/spec/repository'
require 'rdf/elasticsearch'

describe RDF::Elasticsearch::Repository do
  before :all do
    logger = RDF::Spec.logger
    logger.level = Logger::DEBUG
    @load_durable = lambda { RDF::Elasticsearch::Repository.new uri: "http://localhost:9200",
                                                                refresh: true,
                                                                log: false }
    @repository = @load_durable.call
  end

  before :each do
    @repository.clear_statements
  end

  # TODO: refactor this out to a shared example file
  #       and use rdf/spec/literal for values
  shared_examples "a mapped RDF::Term" do
    include Elasticsearch::DSL

    let (:statement) { RDF::Statement.new(RDF::Node.new, RDF::URI.new("urn:predicate:1"), subject) }

    before do
      # create and persist a statement
      @repository.insert_statement(statement)
    end

    it 'should match default query' do
      @repository.has_statement? statement
    end

    it 'should be searchable typed' do
      hash = RDF::Elasticsearch::Conversion.serialize_object(subject)
      field = hash.delete(:type)
      datatype = hash.delete(:datatype)

      body = search do
        query do
          bool do
            must do
              match hash
            end
            unless datatype.nil?
              must do
                match({ datatype: datatype })
              end
            end
          end
        end
      end

      response = @repository.client.search index: @repository.index, body: body.to_hash
      hit = response['hits']['hits'].first

      expect(RDF::Elasticsearch::Conversion.statement_from_es(hit['_type'], hit['_source'])).to eq(statement)
    end

    it 'should be searchable untyped' do
      hash = RDF::Elasticsearch::Conversion.serialize_object(subject)

      body = search do
        query do
          simple_query_string do
            query hash[:o]
          end
        end
      end

      response = @repository.client.search index: @repository.index, body: body.to_hash
      hit = response['hits']['hits'].first

      response = @repository.client.search index: @repository.index, body: body.to_hash
      expect(RDF::Elasticsearch::Conversion.statement_from_es(hit['_type'], hit['_source'])).to eq(statement)
    end
  end

  context 'RDF::Elasticsearch::Mapping' do
    context "with RDF::URI" do
      let (:subject) { RDF::URI.new("urn:object:1") }

      it_behaves_like 'a mapped RDF::Term'
    end

    context "with RDF::Node" do
      let (:subject) { RDF::Node.new('object:1') }

      it_behaves_like 'a mapped RDF::Term'
    end

    context "with RDF::Literal" do
      let (:subject) { RDF::Literal.new('abc') }

      it_behaves_like 'a mapped RDF::Term'
    end

    #
    # Data-typed RDF::Literals
    #

    # boolean
    context "with RDF::Literal::Boolean" do
      let (:subject) { RDF::Literal.new(false, datatype: RDF::XSD.boolean) }

      it_behaves_like 'a mapped RDF::Term'
    end

    # date
    context "with RDF::Literal::Date" do
      let (:subject) { RDF::Literal.new(Date.new(2010), datatype: RDF::XSD.date) }

      it_behaves_like 'a mapped RDF::Term'
    end

    # datetime
    context "with RDF::Literal::DateTime" do
      let (:subject) { RDF::Literal.new(DateTime.new(2011), datatype: RDF::XSD.dateTime) }

      it_behaves_like 'a mapped RDF::Term'
    end

    # decimal
    context "with RDF::Literal::Decimal" do
      let (:subject) { RDF::Literal.new(1.1, datatype: RDF::XSD.decimal) }

      it_behaves_like 'a mapped RDF::Term'
    end

    # double
    context "with RDF::Literal::Double" do
      let (:subject) { RDF::Literal.new(3.1415, datatype: RDF::XSD.double) }

      it_behaves_like 'a mapped RDF::Term'
    end

    # integer
    context "with RDF::Literal::Integer" do
      let (:subject) { RDF::Literal.new(1, datatype: RDF::XSD.integer) }

      it_behaves_like 'a mapped RDF::Term'
    end

    # time
    context "with RDF::Literal::Time" do
      let (:subject) { RDF::Literal.new(Time.now, datatype: RDF::XSD.time) }

      it_behaves_like 'a mapped RDF::Term'
    end

    # token
    context "with RDF::Literal::Token" do
      let (:subject) { RDF::Literal.new(:xyz, datatype: RDF::XSD.token) }

      it_behaves_like 'a mapped RDF::Term'
    end

    #
    # Language-tagged RDF::Literals
    #

    context "with English literal" do
      let (:subject) { RDF::Literal.new('abc', language: 'en') }

      it_behaves_like 'a mapped RDF::Term'
    end

    context "with Finnish literal" do
      let (:subject) { RDF::Literal.new('abc', language: 'fi') }

      it_behaves_like 'a mapped RDF::Term'
    end

    #
    # Custom datatype(s)
    #

    context "with custom-typed literal" do
      let (:subject) { RDF::Literal.new('abc', datatype: RDF::URI.new("urn:datatype:1")) }

      it_behaves_like 'a mapped RDF::Term'
    end
  end

  # @see lib/rdf/spec/repository.rb in RDF-spec
  it_behaves_like "an RDF::Repository" do
    let(:repository) { @repository }
  end
end

