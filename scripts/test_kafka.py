#!/usr/bin/env python3
"""
Script de validación de Kafka.
Produce 10 mensajes de prueba al topic 'test.topic' y los consume.
Uso: python3 scripts/test_kafka.py
"""
import json
import time
import os
from kafka import KafkaProducer, KafkaConsumer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9094")
TOPIC = "test.topic"

def produce():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"[PRODUCER] Conectado a {KAFKA_BROKER}")
    for i in range(10):
        msg = {"id": i, "texto": f"mensaje de prueba {i}", "timestamp": time.time()}
        producer.send(TOPIC, msg)
        print(f"[PRODUCER] Enviado: {msg}")
        time.sleep(0.5)
    producer.flush()
    producer.close()
    print("[PRODUCER] Envío completado.\n")

def consume():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="test-grupo",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    print(f"[CONSUMER] Conectado a {KAFKA_BROKER}, esperando mensajes...")
    count = 0
    for message in consumer:
        print(f"[CONSUMER] Recibido: {message.value}")
        count += 1
        if count >= 10:
            break
    consumer.close()
    print("[CONSUMER] Consumo completado. Kafka OK ✅")

if __name__ == "__main__":
    produce()
    time.sleep(2)
    consume()
