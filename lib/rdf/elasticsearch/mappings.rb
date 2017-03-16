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
            "type": "keyword"
          },
          "p": {
            "type": "keyword"
          },
          "o": {
            "type": "keyword" # this becomes the _all field
          },
          "g": {
            "type": "keyword"
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
            "type": "keyword"
          }
        }
      }

      MAPPINGS['node'] = {
        "properties": {
          "node": {
            "type": "keyword"
          }
        }
      }

      MAPPINGS['literal'] = {
        "properties": {
          "literal": {
            "type": "text",
            "analyzer": "standard",
            "copy_to": "o"
          }
        }
      }

      #
      # XSD datatypes for RDF::Litreral objects
      #

      MAPPINGS['xsd:boolean'] = {
        "properties": {
          "xsd:boolean": {
            "type": "boolean",
            "copy_to": "o"
          }
        }
      }

      MAPPINGS['xsd:date'] = {
        "properties": {
          "xsd:date": {
            "type": "date",
            "copy_to": "o"
          }
        }
      }

      MAPPINGS['xsd:datetime'] = {
        "properties": {
          "xsd:datetime": {
            "type": "date",
            "copy_to": "o"
          }
        }
      }

      MAPPINGS['xsd:decimal'] = {
        "properties": {
          "xsd:decimal": {
            "type": "float",
            "copy_to": "o"
          }
        }
      }

      MAPPINGS['xsd:double'] = {
        "properties": {
          "xsd:double": {
            "type": "double",
            "copy_to": "o"
          }
        }
      }

      MAPPINGS['xsd:integer'] = {
        "properties": {
          "xsd:integer": {
            "type": "long",
            "copy_to": "o"
          }
        }
      }

      MAPPINGS['xsd:time'] = {
        "properties": {
          "xsd:time": {
            "type": "date",
            "format": "HH:mm:ssZ",
            "copy_to": "o"
          }
        }
      }

      MAPPINGS['xsd:token'] = {
        "properties": {
          "xsd:token": {
            "type": "keyword",
            "copy_to": "o"
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
