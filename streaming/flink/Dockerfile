# Build SQL Runner JAR
FROM maven:3.9-eclipse-temurin-17 AS sql-runner-build
WORKDIR /build
COPY sql-runner/pom.xml .
RUN mvn dependency:go-offline -q
COPY sql-runner/src/ src/
RUN mvn clean package -q -DskipTests

FROM flink:1.20-java17

# Download Iceberg Flink runtime JAR
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.7.1/iceberg-flink-runtime-1.20-1.7.1.jar

# Download Iceberg AWS bundle JAR (S3FileIO + AWS SDK for MinIO access)
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar

# Download Hadoop uber JAR (provides org.apache.hadoop.conf.Configuration for Iceberg REST catalog)
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# Download Flink SQL Kafka connector JAR
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar

# Download Flink SQL Avro Confluent Registry format JAR (Avro + Schema Registry client)
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.20.0/flink-sql-avro-confluent-registry-1.20.0.jar

# Enable S3 filesystem plugin for checkpoint storage on MinIO
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

# Copy SQL files into the image
COPY sql/ /opt/flink/sql/

# Copy SQL Runner JAR for Application Mode
COPY --from=sql-runner-build /build/target/flink-sql-runner.jar /opt/flink/usrlib/flink-sql-runner.jar

# Copy K8s entrypoint for endpoint parameterization
COPY k8s-entrypoint.sh /opt/k8s-entrypoint.sh
RUN chmod +x /opt/k8s-entrypoint.sh
