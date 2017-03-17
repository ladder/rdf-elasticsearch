require 'pry'

module RDF
  module Elasticsearch
    class Mappings

      MAPPINGS = Hash.new

      # These are the various RDF::Literal data types
      #
      # Index mappings exist to tell Elasticsearch how to handle
      # (eg. index, analyze, store) data for each type

      # MAPPINGS['uri']     = { "type": "keyword" }
      # MAPPINGS['node']    = { "type": "keyword" }
      # MAPPINGS['token']   = { "type": "keyword" }
      MAPPINGS['literal'] = { "type": "text" }
      # MAPPINGS['typed']   = { "type": "keyword" }

      #
      # XSD datatypes for RDF::Literal objects
      #

      MAPPINGS['boolean']  = { "type": "boolean" }
      MAPPINGS['date']     = { "type": "date" }
      MAPPINGS['dateTime'] = { "type": "date" }
      MAPPINGS['decimal']  = { "type": "float" }
      MAPPINGS['double']   = { "type": "double" }
      MAPPINGS['integer']  = { "type": "integer" }
      MAPPINGS['time']     = { "type": "date", "format": "HH:mm:ssZ" }

      #
      # Language-typed literal analyzers (from Elastic)
      #
      # see: https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-lang-analyzer.html
      #

      MAPPINGS['lang_ara'] = { "type": "text", "analyzer": "arabic" }
      MAPPINGS['lang_arm'] = { "type": "text", "analyzer": "armenian" }
      MAPPINGS['lang_baq'] = { "type": "text", "analyzer": "basque" }
      MAPPINGS['lang_bra'] = { "type": "text", "analyzer": "brazilian" }
      MAPPINGS['lang_bul'] = { "type": "text", "analyzer": "bulgarian" }
      MAPPINGS['lang_cat'] = { "type": "text", "analyzer": "catalan" }
      MAPPINGS['lang_cze'] = { "type": "text", "analyzer": "czech" }
      MAPPINGS['lang_dan'] = { "type": "text", "analyzer": "danish" }
      MAPPINGS['lang_dut'] = { "type": "text", "analyzer": "dutch" }
      MAPPINGS['lang_eng'] = { "type": "text", "analyzer": "english" }
      MAPPINGS['lang_fin'] = { "type": "text", "analyzer": "finnish" }
      MAPPINGS['lang_fre'] = { "type": "text", "analyzer": "french" }
      MAPPINGS['lang_glg'] = { "type": "text", "analyzer": "galician" }
      MAPPINGS['lang_ger'] = { "type": "text", "analyzer": "german" }
      MAPPINGS['lang_grc'] = { "type": "text", "analyzer": "greek" }
      MAPPINGS['lang_hin'] = { "type": "text", "analyzer": "hindi" }
      MAPPINGS['lang_hun'] = { "type": "text", "analyzer": "hungarian" }
      MAPPINGS['lang_ind'] = { "type": "text", "analyzer": "indonesian" }
      MAPPINGS['lang_gle'] = { "type": "text", "analyzer": "irish" }
      MAPPINGS['lang_ita'] = { "type": "text", "analyzer": "italian" }
      MAPPINGS['lang_lit'] = { "type": "text", "analyzer": "lithuanian" }
      MAPPINGS['lang_nor'] = { "type": "text", "analyzer": "norwegian" }
      MAPPINGS['lang_per'] = { "type": "text", "analyzer": "persian" }
      MAPPINGS['lang_por'] = { "type": "text", "analyzer": "portuguese" }
      MAPPINGS['lang_ron'] = { "type": "text", "analyzer": "romanian" }
      MAPPINGS['lang_rus'] = { "type": "text", "analyzer": "russian" }
      MAPPINGS['lang_ckb'] = { "type": "text", "analyzer": "sorani" }
      MAPPINGS['lang_spa'] = { "type": "text", "analyzer": "spanish" }
      MAPPINGS['lang_swe'] = { "type": "text", "analyzer": "swedish" }
      MAPPINGS['lang_tur'] = { "type": "text", "analyzer": "turkish" }
      MAPPINGS['lang_tha'] = { "type": "text", "analyzer": "thai" }

      #
      # Return an index mapping
      #
      def self.index
        mappings = Hash.new

        mappings['_default_'] = {
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

        mappings['typed'] = {
          "properties": {
            "datatype": {
              "type": "keyword" # for typed literals
            }
          }
        }

        MAPPINGS.each do |type, body|
          mappings[type] = {
            "properties": {
              "o": {
                "type": "keyword",
                "fields": {
                  type => body
                }
              }
            }
          }
        end

        { "mappings": mappings }
      end
    end
  end
end
