#!/usr/bin/env python3
"""
Productor Binance → Kafka

Se conecta al WebSocket público de Binance (combined stream) para:
  - BTCUSDT @miniTicker
  - ETHUSDT @miniTicker
  - BTCUSDT @kline_1m
  - ETHUSDT @kline_1m

Publica en los topics Kafka:
  - miniTicker
  - kline

Incluye:
  - Reconexión automática con backoff exponencial
  - Métricas Prometheus (precio actual, mensajes enviados)
  - Formato JSON estructurado con timestamp normalizado
"""
import json
import os
import sys
import signal
import time
import threading
import logging
from datetime import datetime, timezone

import websocket
from kafka import KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Gauge, Counter, start_http_server

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9094")
TOPIC_MINI_TICKER = os.getenv("KAFKA_TOPIC_MINI_TICKER", "miniTicker")
TOPIC_KLINE = os.getenv("KAFKA_TOPIC_KLINE", "kline")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

PAIRS = ["btcusdt", "ethusdt"]
STREAMS = "/".join([f"{p}@miniTicker/{p}@kline_1m" for p in PAIRS])
WS_URL = f"wss://stream.binance.com:9443/stream?streams={STREAMS}"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("binance-producer")

# ---------------------------------------------------------------------------
# Métricas Prometheus
# ---------------------------------------------------------------------------
PRECIO_ACTUAL = Gauge(
    "crypto_precio_actual",
    "Precio actual del par",
    ["par", "tipo_stream"],
)
MENSAJES_ENVIADOS = Counter(
    "pipeline_mensajes_enviados_total",
    "Total de mensajes enviados a Kafka",
    ["topic"],
)
WS_RECONNECTS = Counter(
    "websocket_reconexiones_total",
    "Total de reconexiones al WebSocket de Binance",
)

# ---------------------------------------------------------------------------
# Kafka Producer
# ---------------------------------------------------------------------------
class KafkaPublisher:
    def __init__(self, bootstrap_servers: str):
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            retries=3,
            acks="all",
        )
        logger.info("KafkaProducer conectado a %s", bootstrap_servers)

    def send(self, topic: str, key: str, value: dict):
        try:
            future = self.producer.send(topic, key=key, value=value)
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            MENSAJES_ENVIADOS.labels(topic=topic).inc()
        except KafkaError as e:
            logger.error("Error enviando a Kafka: %s", e)

    def _on_send_success(self, record_metadata):
        logger.debug(
            "Kafka OK → topic=%s partition=%s offset=%s",
            record_metadata.topic,
            record_metadata.partition,
            record_metadata.offset,
        )

    def _on_send_error(self, excp):
        logger.error("Kafka ERROR: %s", excp)

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.close()

# ---------------------------------------------------------------------------
# Binance WebSocket Handler
# ---------------------------------------------------------------------------
class BinanceProducer:
    def __init__(self):
        self.kafka = KafkaPublisher(KAFKA_BROKER)
        self.ws = None
        self.running = True
        self.reconnect_delay = 1  # segundos (backoff exponencial)
        self.max_reconnect_delay = 60

        # Graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    # --- Prometheus metrics server -----------------------------------------
    def start_metrics_server(self):
        def _serve():
            start_http_server(METRICS_PORT)
            logger.info("Servidor de métricas Prometheus en puerto %s", METRICS_PORT)

        t = threading.Thread(target=_serve, daemon=True)
        t.start()

    # --- WebSocket callbacks -----------------------------------------------
    def on_open(self, ws):
        logger.info("WebSocket ABIERTO → %s", WS_URL)
        self.reconnect_delay = 1  # reset backoff

    def on_message(self, ws, raw_message: str):
        try:
            payload = json.loads(raw_message)
            stream = payload.get("stream", "")
            data = payload.get("data", {})

            # Determinar topic y par
            if "@miniTicker" in stream:
                topic = TOPIC_MINI_TICKER
                par = stream.replace("@miniTicker", "").upper()
                tipo_stream = "miniTicker"
                # Actualizar métrica de precio
                precio = data.get("c")
                if precio:
                    PRECIO_ACTUAL.labels(par=par, tipo_stream=tipo_stream).set(float(precio))
            elif "@kline_1m" in stream:
                topic = TOPIC_KLINE
                par = stream.replace("@kline_1m", "").upper()
                tipo_stream = "kline_1m"
            else:
                logger.warning("Stream desconocido: %s", stream)
                return

            # Enriquecer mensaje
            enriched = {
                "timestamp": time.time(),
                "par": par,
                "tipo_stream": tipo_stream,
                "datos": data,
            }

            self.kafka.send(topic, key=par, value=enriched)
            logger.debug("Enviado a %s → %s", topic, par)

        except json.JSONDecodeError:
            logger.error("JSON inválido recibido: %s", raw_message)
        except Exception as e:
            logger.error("Error procesando mensaje: %s", e)

    def on_error(self, ws, error):
        logger.error("WebSocket ERROR: %s", error)

    def on_close(self, ws, close_status_code, close_msg):
        logger.warning(
            "WebSocket CERRADO (code=%s, msg=%s)", close_status_code, close_msg
        )
        if self.running:
            self._reconnect()

    # --- Conexión y reconexión ---------------------------------------------
    def connect(self):
        logger.info("Conectando a Binance WebSocket...")
        self.ws = websocket.WebSocketApp(
            WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        self.ws.run_forever()

    def _reconnect(self):
        WS_RECONNECTS.inc()
        logger.info(
            "Reconexión en %s segundos...", self.reconnect_delay
        )
        time.sleep(self.reconnect_delay)
        self.reconnect_delay = min(
            self.reconnect_delay * 2, self.max_reconnect_delay
        )
        self.connect()

    def _shutdown(self, signum, frame):
        logger.info("Señal de apagado recibida. Cerrando...")
        self.running = False
        if self.ws:
            self.ws.close()
        self.kafka.flush()
        self.kafka.close()
        sys.exit(0)

    # --- Main loop ---------------------------------------------------------
    def run(self):
        self.start_metrics_server()
        self.connect()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info(" Binance Producer → Kafka")
    logger.info(" Pares: %s", ", ".join(p.upper() for p in PAIRS))
    logger.info(" Topics: %s, %s", TOPIC_MINI_TICKER, TOPIC_KLINE)
    logger.info(" Kafka Broker: %s", KAFKA_BROKER)
    logger.info("=" * 60)
    producer = BinanceProducer()
    producer.run()
