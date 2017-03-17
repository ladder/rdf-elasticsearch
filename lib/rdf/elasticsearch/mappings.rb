require 'pry'

module RDF
  module Elasticsearch
    class Mappings

      MAPPINGS = Hash.new

      # Base MAPPINGS for subject, predicate, graph_name
      # FIXME: send _default_ type separately?
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
          "o": {
            "type": "keyword",
            "fields": {
              "uri": {
                "type": "keyword"
              }
            }
          }
        }
      }

      MAPPINGS['node'] = {
        "properties": {
          "o": {
            "type": "keyword",
            "fields": {
              "node": {
                "type": "keyword"
              }
            }
          }
        }
      }

      MAPPINGS['token'] = {
        "properties": {
          "o": {
            "type": "keyword",
            "fields": {
              "token": {
                "type": "keyword"
              }
            }
          }
        }
      }

      MAPPINGS['literal'] = {
        "properties": {
          "o": {
            "type": "keyword",
            "fields": {
              "literal": {
                "type": "text"
              }
            }
          }
        }
      }

      # for typed literals
      MAPPINGS['typed'] = {
        "properties": {
          "o": {
            "type": "keyword",
            "fields": {
              "typed": {
                "type": "keyword"
              }
            }
          },
          "datatype": {
            "type": "keyword"
          }
        }
      }

      #
      # XSD datatypes for RDF::Litreral objects
      #

      MAPPINGS['xsd:boolean'] = {
        "properties": {
          "o": {
            "type": "keyword",
            "fields": {
              "xsd:boolean": {
                "type": "boolean"
              }
            }
          }
        }
      }

      MAPPINGS['xsd:date'] = {
        "properties": {
          "o": {
            "type": "keyword",
            "fields": {
              "xsd:date": {
                "type": "date"
              }
            }
          }
        }
      }

      MAPPINGS['xsd:dateTime'] = {
        "properties": {
          "o": {
            "type": "keyword",
            "fields": {
              "xsd:dateTime": {
                "type": "date"
              }
            }
          }
        }
      }

      MAPPINGS['xsd:decimal'] = {
        "properties": {
          "o": {
            "type": "keyword",
            "fields": {
              "xsd:decimal": {
                "type": "float"
              }
            }
          }
        }
      }

      MAPPINGS['xsd:double'] = {
        "properties": {
          "o": {
            "type": "keyword",
            "fields": {
              "xsd:double": {
                "type": "double"
              }
            }
          }
        }
      }

      MAPPINGS['xsd:integer'] = {
        "properties": {
          "o": {
            "type": "keyword",
            "fields": {
              "xsd:integer": {
                "type": "integer"
              }
            }
          }
        }
      }

      MAPPINGS['xsd:time'] = {
        "properties": {
          "o": {
            "type": "keyword",
            "fields": {
              "xsd:time": {
                "type": "date",
                "format": "HH:mm:ssZ"
              }
            }
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
          "o": {
            "type": "keyword",
            "fields": {
              "lang_en": {
                "type": "text",
                "analyzer": "english"
              }
            }
          }
        }
      }

      MAPPINGS['lang_fi'] = {
        "properties": {
          "o": {
            "type": "keyword",
            "fields": {
              "lang_fi": {
                "type": "text",
                "analyzer": "finnish"
              }
            }
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
