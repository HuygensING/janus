#!/bin/sh
#
# Create index for annotations.

base=http://localhost:9200

curl -X PUT "$base/janus_annotations" -H "Content-Type: application/json" -d '{
  "mappings": {
    "annotation": {
      "_all": {"enabled": false},
      "dynamic": "strict",
      "properties": {
        "body": {"type": "text"},
        "attrib": {
          "type": "nested",
          "dynamic": true
        },
        "root": {"type": "keyword"},
        "target": {"type": "keyword"},
        "type": {"type": "keyword"},
        "start": {"type": "integer"},
        "end": {"type": "integer"},
        "order": {"type": "integer"}
      }
    }
  }
}'
