#!/bin/sh
#
# Create index for annotations.

base="http://${1:-localhost}:9200"

curl -X PUT "$base/janus_annotations" -H "Content-Type: application/json" \
    -d @"$(dirname $0)/annotation-mapping.json"
