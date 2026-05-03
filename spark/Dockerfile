FROM apache/spark:3.5.0

USER root

WORKDIR /app

# Instalar dependencia Python para métricas
RUN pip install --no-cache-dir prometheus-client==0.20.0

COPY apps/crypto_streaming.py .

ENV KAFKA_BROKER=kafka:9092
ENV KAFKA_TOPIC_MINI_TICKER=miniTicker
ENV KAFKA_TOPIC_KLINE=kline
ENV HDFS_NAMENODE_URI=hdfs://namenode:9000
ENV HDFS_BASE_PATH=/data/cripto
ENV METRICS_PORT=8001

EXPOSE 8001

CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "--master", "spark://spark-master:7077", \
     "/app/crypto_streaming.py"]
