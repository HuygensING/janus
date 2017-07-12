FROM elasticsearch:5.4-alpine
ENV ES_JAVA_OPTS="-Xms512m -Xmx512m"
COPY elasticsearch.yml /usr/share/elasticsearch/config/
# Needed for healthcheck in docker-compose.yml
RUN apk add --no-cache curl
