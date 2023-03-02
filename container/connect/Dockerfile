FROM debezium/connect:latest

USER kafka

RUN curl --create-dirs -LO --output-dir /tmp/connector https://github.com/aiven/s3-connector-for-apache-kafka/releases/download/v2.12.0/aiven-kafka-connect-s3-2.12.0.zip && \
    unzip -o /tmp/connector/aiven-kafka-connect-s3-2.12.0.zip -d /tmp/connector && \
    mv /tmp/connector/aiven-kafka-connect-s3-2.12.0 /kafka/connect/aiven-kafka-connect-s3 && \
    rm /tmp/connector/aiven-kafka-connect-s3-2.12.0.zip