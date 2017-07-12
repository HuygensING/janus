FROM elasticsearch:5.4-alpine
COPY elasticsearch.yml /usr/share/elasticsearch/config/
# Needed for healthcheck in docker-compose.yml
RUN apk add --no-cache curl
