#!/bin/sh

set -e

base=http://localhost:8080
json='Content-Type: application/json'

docid=$(curl "$base"/put -d 'Hello, world!')
annid1=$(curl -H "$json" "$base"/annotate/$docid -d '{"start": 0, "end": 5}')
annid2=$(curl -H "$json" "$base"/annotate/$annid1 -d '{"start": 0, "end": 5}')
annid3=$(curl -H "$json" "$base"/annotate/$annid2 -d '{"start": 0, "end": 5}')

curl -s "$base"/get/$docid | jq '.annotations | length'     # 2
curl -s "$base"/get/$annid1 | jq '.annotations | length'    # 1
