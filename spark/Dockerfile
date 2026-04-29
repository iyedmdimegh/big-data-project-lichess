FROM bitnamilegacy/spark:3.5

USER root

RUN apt-get update && apt-get install -y --no-install-recommends wget && \
    rm -rf /var/lib/apt/lists/*

# Bitnami Spark images run as UID 1001 with no /etc/passwd entry, which breaks
# Hadoop's JAAS UnixLoginModule (NPE on getpwuid). Add a user entry so Spark
# Structured Streaming jobs that touch HDFS APIs (checkpoints, file sources)
# can resolve the current user.
RUN echo 'spark:x:1001:0:Spark user:/tmp:/bin/bash' >> /etc/passwd

RUN python3 -m pip install --no-cache-dir \
    pyspark \
    numpy \
    kafka-python \
    python-chess \
    cassandra-driver \
    psycopg2-binary \
    elasticsearch \
    faker

# Add Kafka connector jars so Spark Structured Streaming can read Kafka without --packages.
RUN wget -q -O /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \
    wget -q -O /opt/bitnami/spark/jars/kafka-clients-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar && \
    wget -q -O /opt/bitnami/spark/jars/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    wget -q -O /opt/bitnami/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar

# Cassandra connector (assembly = shaded driver, avoids classpath conflicts) + Postgres JDBC.
RUN wget -q -O /opt/bitnami/spark/jars/spark-cassandra-connector-assembly_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.5.1/spark-cassandra-connector-assembly_2.12-3.5.1.jar && \
    wget -q -O /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

USER 1001
