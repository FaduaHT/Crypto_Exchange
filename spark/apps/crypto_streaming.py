#!/usr/bin/env python3
"""
Spark Structured Streaming — CryptoExchange Pipeline

Lee de Kafka (topics miniTicker, kline), calcula indicadores técnicos
y escribe el histórico en HDFS (Parquet) particionado por par y fecha.

Indicadores implementados:
  1. SMA (Simple Moving Average) de 5 minutos, slide 1 minuto.
  2. Variación porcentual intra-ventana (volatilidad).

Métricas Prometheus expuestas en :8001/metrics
"""
import os
import sys
import signal
import threading
import time
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, schema_of_json, window, avg, max, min,
    current_date, lit, to_date, from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, MapType
)
from prometheus_client import Gauge, Counter, start_http_server

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC_MINI_TICKER = os.getenv("KAFKA_TOPIC_MINI_TICKER", "miniTicker")
TOPIC_KLINE = os.getenv("KAFKA_TOPIC_KLINE", "kline")
HDFS_URI = os.getenv("HDFS_NAMENODE_URI", "hdfs://namenode:9000")
HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/data/cripto")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8001"))
CHECKPOINT_DIR = f"{HDFS_BASE_PATH}/_checkpoints"

# ---------------------------------------------------------------------------
# Métricas Prometheus
# ---------------------------------------------------------------------------
MENSAJES_PROCESADOS = Counter(
    "pipeline_mensajes_procesados_total",
    "Mensajes procesados por Spark Streaming",
    ["topic", "par"],
)
BATCHES_PROCESADOS = Counter(
    "spark_batches_procesados_total",
    "Micro-batches procesados correctamente",
)
PRECIO_SMA = Gauge(
    "crypto_precio_sma",
    "SMA del precio en la ventana configurada",
    ["par"],
)
VARIACION_PCT = Gauge(
    "crypto_variacion_pct",
    "Variación porcentual intra-ventana",
    ["par"],
)

# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("CryptoStreaming")
    .master("spark://spark-master:7077")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Esquemas JSON
# ---------------------------------------------------------------------------
schema_mini = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("par", StringType(), True),
    StructField("tipo_stream", StringType(), True),
    StructField("datos", MapType(StringType(), StringType()), True),
])

schema_kline = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("par", StringType(), True),
    StructField("tipo_stream", StringType(), True),
    StructField("datos", MapType(StringType(), StringType()), True),
])

# ---------------------------------------------------------------------------
# Lectura Kafka — miniTicker
# ---------------------------------------------------------------------------
raw_mini = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_MINI_TICKER)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed_mini = (
    raw_mini
    .select(from_json(col("value").cast("string"), schema_mini).alias("v"))
    .select("v.*")
    .withColumn("precio", col("datos.c").cast(DoubleType()))
    .withColumn("event_time", from_unixtime(col("timestamp")).cast("timestamp"))
    .filter(col("precio").isNotNull())
)

# ---------------------------------------------------------------------------
# Indicadores sobre miniTicker (ventana de 5 min, slide 1 min)
# ---------------------------------------------------------------------------
indicadores_mini = (
    parsed_mini
    .withWatermark("event_time", "1 minute")
    .groupBy(
        window(col("event_time"), "2 minutes", "30 seconds"),
        col("par"),
    )
    .agg(
        avg("precio").alias("sma_5min"),
        max("precio").alias("max_precio"),
        min("precio").alias("min_precio"),
        avg("timestamp").alias("avg_timestamp"),
    )
    .withColumn(
        "variacion_pct",
        ((col("max_precio") - col("min_precio")) / col("min_precio")) * 100
    )
    .withColumn("fecha", to_date(from_unixtime(col("avg_timestamp"))))
    .select("par", "fecha", "sma_5min", "variacion_pct", "max_precio", "min_precio")
)

# ---------------------------------------------------------------------------
# Lectura Kafka — kline
# ---------------------------------------------------------------------------
raw_kline = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_KLINE)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed_kline = (
    raw_kline
    .select(from_json(col("value").cast("string"), schema_kline).alias("v"))
    .select("v.*")
    .withColumn("fecha", to_date(from_unixtime(col("timestamp"))))
    .select("par", "fecha", "timestamp", "tipo_stream", "datos")
)

# ---------------------------------------------------------------------------
# Escritura HDFS — miniTicker con indicadores
# ---------------------------------------------------------------------------
def escribir_mini(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    BATCHES_PROCESADOS.inc()
    # Actualizar métricas por par
    for row in batch_df.collect():
        par = row.par
        sma = row.sma_5min
        var = row.variacion_pct
        if sma is not None:
            PRECIO_SMA.labels(par=par).set(float(sma))
        if var is not None:
            VARIACION_PCT.labels(par=par).set(float(var))
        MENSAJES_PROCESADOS.labels(topic=TOPIC_MINI_TICKER, par=par).inc()

    batch_df.write.mode("append").parquet(f"{HDFS_URI}{HDFS_BASE_PATH}/miniTicker")

query_mini = (
    indicadores_mini.writeStream
    .foreachBatch(escribir_mini)
    .outputMode("append")
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/mini")
    .start()
)

# ---------------------------------------------------------------------------
# Escritura HDFS — kline raw (histórico de velas)
# ---------------------------------------------------------------------------
def escribir_kline(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    BATCHES_PROCESADOS.inc()
    for row in batch_df.collect():
        MENSAJES_PROCESADOS.labels(topic=TOPIC_KLINE, par=row.par).inc()
    batch_df.write.mode("append").parquet(f"{HDFS_URI}{HDFS_BASE_PATH}/kline")

query_kline = (
    parsed_kline.writeStream
    .foreachBatch(escribir_kline)
    .outputMode("append")
    .trigger(processingTime="60 seconds")
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/kline")
    .start()
)

# ---------------------------------------------------------------------------
# Métricas HTTP server (hilo daemon)
# ---------------------------------------------------------------------------
def start_metrics_server():
    start_http_server(METRICS_PORT)
    print(f"[METRICS] Servidor Prometheus en puerto {METRICS_PORT}")

threading.Thread(target=start_metrics_server, daemon=True).start()

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
def shutdown(signum, frame):
    print("\n[SHUTDOWN] Cerrando queries Spark...")
    query_mini.stop()
    query_kline.stop()
    spark.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ---------------------------------------------------------------------------
# Esperar
# ---------------------------------------------------------------------------
print("[SPARK] Streaming iniciado. Esperando datos...")
query_mini.awaitTermination()
