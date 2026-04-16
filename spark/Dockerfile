FROM bitnamilegacy/spark:3.5

USER root

RUN apt-get update && apt-get install -y --no-install-recommends wget && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir \
    pyspark \
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
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.0/kafka-clients-3.5.0.jar

USER 1001
