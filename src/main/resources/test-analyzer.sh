#!/bin/sh

# Utility to quickly test new analyzer settings.

index="http://localhost:9200/${1:-test}"

jq '{"settings": .}' "$(dirname $0)/analyzer.json" |
    curl -X PUT "$index" -d @-

curl -X POST "$index"/doc -d '{
    "body": "Île de la cité"
}'

curl -X POST "$index"/doc -d '{
    "body": "L'\''Île-Rousse"
}'

curl -X POST "$index"/doc -d '{
    "body": "L'\''escargot"
}'

sleep 1

curl "$index/_search" -d '{
    "query": {
        "query_string": {
            "query": "ile"
        }
    }
}'
