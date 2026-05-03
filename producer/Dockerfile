FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY binance_producer.py .

ENV KAFKA_BROKER=kafka:9092
ENV KAFKA_TOPIC_MINI_TICKER=miniTicker
ENV KAFKA_TOPIC_KLINE=kline
ENV METRICS_PORT=8000
ENV LOG_LEVEL=INFO

EXPOSE 8000

CMD ["python3", "binance_producer.py"]
