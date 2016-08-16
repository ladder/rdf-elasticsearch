$:.unshift "."
require 'spec_helper'

require 'rdf/spec/repository'
require 'rdf/elasticsearch'

describe RDF::Elasticsearch::Repository do
  before :all do
    logger = RDF::Spec.logger
    logger.level = Logger::DEBUG
    @load_durable = lambda { RDF::Elasticsearch::Repository.new uri: "http://localhost:9200", logger: logger }
    @repository = @load_durable.call
  end

  before :each do
    @repository.clear!
  end

  after :each do
    @repository.clear!
  end

  # @see lib/rdf/spec/repository.rb in RDF-spec
  it_behaves_like "an RDF::Repository" do
    let(:repository) {@repository}
  end
end

