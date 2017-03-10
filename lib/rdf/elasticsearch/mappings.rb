require 'pry'

module RDF
  module Elasticsearch
    class Mappings

      MAPPINGS = Hash.new

      # Base MAPPINGS for subject, predicate, graph_name
      MAPPINGS['_default_'] = 
      {
        "_all": {
          "enabled": false
        },
        "properties": {
          "s": {
            "type": "string",
            "index": "not_analyzed"
          },
          "p": {
            "type": "string",
            "index": "not_analyzed"
          },
          "o": {
            "type": "string",
            "index": "not_analyzed"
          },
          "g": {
            "type": "string",
            "index": "not_analyzed"
          }          
        }
      }

      # These are the various RDF::Literal data types
      #
      # Index mappings exist to tell Elasticsearch how to handle
      # (eg. index, analyze, store) data for each type
      MAPPINGS['uri'] = {
        "properties": {
          "uri": {
            "type": "string",
            "index": "not_analyzed",
            "copy_to": "o"
          }
        }
      }

      MAPPINGS['node'] = {
        "properties": {
          "uri": {
            "type": "string",
            "index": "not_analyzed",
            "copy_to": "o"
          }
        }
      }

      # TODO: does this just go in object?
      MAPPINGS['literal'] = {
        "properties": {
          "uri": {
            "type": "string",
            "index": "not_analyzed",
            "copy_to": "o"
          }
        }
      }

=begin
      MAPPINGS[:lang_en] = {}

      MAPPINGS[:boolean] = {}
      MAPPINGS[:date] = {}
      MAPPINGS[:datetime] = {}
      MAPPINGS[:decimal] = {}
      MAPPINGS[:double] = {}
      MAPPINGS[:integer] = {}
      MAPPINGS[:numeric] = {}
      MAPPINGS[:time] = {}
      MAPPINGS[:token] = {}
=end
      
      def self.ensure_mappings(data)
        MAPPINGS.each do |type, body|
          data.client.indices.put_mapping index: data.index, type: type, body: body
        end
      end
    end
  end
end
