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
=begin
        "dynamic_templates": [
          {
            "string_not_analyzed": {
              "match": "*",
              "mapping": {
                "type": "text",
                "index": "no"
              }
            }
          }
        ],
=end
        "properties": {
          "s": {
            "type": "text"
          },
          "p": {
            "type": "text"
          },
          "o": {
            "type": "text"
          },
          "g": {
            "type": "text"
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
            "type": "text",
            "analyzer": "keyword"
          }
        }
      }

      MAPPINGS['node'] = {
        "properties": {
          "node": {
            "type": "text",
            "analyzer": "keyword"
          }
        }
      }

      # TODO: does this just go in object?
      MAPPINGS['literal'] = {
        "properties": {
          "literal": {
            "type": "text",
            "analyzer": "standard"
          }
        }
      }

      #
      # XSD datatypes for RDF::Litreral objects
      #

      MAPPINGS['xsd:boolean'] = {
        "properties": {
          "xsd:boolean": {
            "type": "boolean"
          }
        }
      }

      MAPPINGS['xsd:date'] = {
        "properties": {
          "xsd:date": {
            "type": "date"
          }
        }
      }

      MAPPINGS['xsd:datetime'] = {
        "properties": {
          "xsd:datetime": {
            "type": "date"
          }
        }
      }

      MAPPINGS['xsd:decimal'] = {
        "properties": {
          "xsd:decimal": {
            "type": "float"
          }
        }
      }

      MAPPINGS['xsd:double'] = {
        "properties": {
          "xsd:double": {
            "type": "double"
          }
        }
      }

      MAPPINGS['xsd:integer'] = {
        "properties": {
          "xsd:integer": {
            "type": "long"
          }
        }
      }

      MAPPINGS['xsd:time'] = {
        "properties": {
          "xsd:time": {
            "type": "date"
          }
        }
      }

      MAPPINGS['xsd:token'] = {
        "properties": {
          "xsd:token": {
            "type": "keyword"
          }
        }
      }

      #
      # Language-typed literal analyzers (from Elastic)
      #
      # Arabic, Armenian, Basque, Brazilian, Bulgarian, Catalan, Chinese, Czech, Danish, Dutch, English, Finnish, French, Galician, German, Greek, Hindi, Hungarian, Indonesian, Irish, Italian, Japanese, Korean, Kurdish, Norwegian, Persian, Portuguese, Romanian, Russian, Spanish, Swedish, Turkish, and Thai.
      #

      MAPPINGS['lang_en'] = {
        "properties": {
          "lang_en": {
            "type": "text",
            "analyzer": "english"
          }
        }
      }

      MAPPINGS['lang_fi'] = {
        "properties": {
          "lang_fi": {
            "type": "text",
            "analyzer": "finnish"
          }
        }
      }

      def self.ensure_mappings(data)
        MAPPINGS.each do |type, body|
          data.client.indices.put_mapping index: data.index, type: type, body: body
        end
      end
    end
  end
end
