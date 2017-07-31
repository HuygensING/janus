FROM maven:3.3-jdk-8-alpine

WORKDIR /build

COPY pom.xml pom.xml
COPY src/main src/main

WORKDIR /

RUN cd /build \
 && mvn clean package \
 && rm -f target/appassembler/bin/*.bat \
 && cp -R target/appassembler/* /usr/local \
 && cd / \
 && rm -rf /build

COPY config-docker-compose.yml .
COPY config-template.yml config.yml
COPY annotation-mapping.json .
COPY put_index.sh .

EXPOSE 8080 8081

ENTRYPOINT ["/usr/local/bin/janus", "server", "config.yml"]
