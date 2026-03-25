FROM gradle:8.10-jdk17 AS builder
WORKDIR /build
COPY . .
RUN gradle :delta-connect-dist:shadowJar --no-daemon

FROM confluentinc/cp-kafka-connect:7.6.0

COPY --from=builder \
    /build/delta-connect-dist/build/libs/delta-sink-connector-*-all.jar \
    /usr/share/java/delta-sink-connector/

ENV CONNECT_PLUGIN_PATH="/usr/share/java"

LABEL org.opencontainers.image.title="Delta Sink Connector" \
    org.opencontainers.image.description="Kafka Connect sink connector for Delta Lake" \
    org.opencontainers.image.source="https://github.com/deltaconnect/delta-sink-connector"
