FROM apache/kafka:3.9.0

LABEL maintainer="your-email@example.com"

USER root
RUN apk update && apk add --no-cache curl libaio tar

ENV DEBEZIUM_VERSION="3.0.7.Final" \
    PLUGIN_DIR="/kafka/connect"

RUN curl -L -k https://repo1.maven.org/maven2/io/debezium/debezium-connector-mysql/${DEBEZIUM_VERSION}/debezium-connector-mysql-${DEBEZIUM_VERSION}-plugin.tar.gz -o /tmp/debezium-mysql-connector.tar.gz && \
    curl -L -k https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz -o /tmp/debezium-postgres-connector.tar.gz

RUN mkdir -p ${PLUGIN_DIR} && \
tar -xvzf /tmp/debezium-mysql-connector.tar.gz -C ${PLUGIN_DIR} && \
tar -xvzf /tmp/debezium-postgres-connector.tar.gz -C ${PLUGIN_DIR} && \
rm /tmp/debezium-*-connector.tar.gz


ENV KAFKA_CONNECT_PLUGIN_PATH=${PLUGIN_DIR}

EXPOSE 9093 8083

COPY start.sh /usr/sbin/start
COPY connect-standalone.properties /opt/kafka/config/connect-standalone.properties
RUN chmod +x /usr/sbin/start

ENTRYPOINT ["/__cacert_entrypoint.sh"]
CMD ["start"]

