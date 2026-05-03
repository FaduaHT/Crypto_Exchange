# Decisiones de Diseño — CryptoExchange Pipeline

> Documento vivo. Se actualiza en cada fase del proyecto.

## 1. Arquitectura General

- **Stack:** Binance WebSocket → Python Producer → Kafka → Spark Streaming → HDFS (Parquet) + Grafana (tiempo real).
- **Orquestación:** Docker Compose con red bridge personalizada (`crypto-network`).
- **Entorno:** Clúster local de un solo nodo. Todas las decisiones de replicación/particionado se adaptan a esta restricción.

## 2. Fuentes y Pares (Binance)

| Par | Stream | Justificación |
|-----|--------|---------------|
| BTC/USDT | `@miniTicker`, `@kline_1m` | Obligatorio por enunciado. Mayor liquidez y volatilidad del mercado. |
| ETH/USDT | `@miniTicker`, `@kline_1m` | Libre elección. Elegido por ser el segundo par más líquido, estable y representativo del mercado altcoin. |

- **Descarte de `@trade` y `@depth`:** El enunciado advierte explícitamente de que inundan el bus. Se prioriza estabilidad del pipeline sobre granularidad.

## 3. Ingesta — Apache Kafka

### Diseño de Topics

| Topic | Contenido | Particiones (propuesta) |
|-------|-----------|------------------------|
| `miniTicker` | Precio actual, volumen 24h, % cambio. Cada ~1s. | 2 (una por par, lógicamente) |
| `kline` | Velas OHLCV de 1 minuto. | 2 |

**Justificación de un topic por tipo de dato (vs. uno general o uno por par):**
- **Escalabilidad:** Si en el futuro se añaden más pares, no proliferan los topics.
- **Separación de responsabilidades:** Spark puede suscribirse solo al topic que necesita para un job concreto (ej. indicadores técnicos solo necesitan `kline`).
- **Gestión sencilla:** Retención y configuración distinta por tipo de dato (las velas ocupan más que los ticks).
- **Desventaja:** El consumidor debe filtrar por par si solo quiere uno. Aceptable dado que solo usamos 2 pares.

### Formato del Mensaje

```json
{
  "timestamp": 1714300000.123,
  "par": "BTCUSDT",
  "tipo_stream": "miniTicker",
  "datos": { ... payload original de Binance ... }
}
```

- Se añade `timestamp` de recepción para normalizar series temporales.
- Se añade `par` mayúsculas para consistencia en el particionado HDFS.

## 4. Almacenamiento — HDFS

| Decisión | Valor | Justificación |
|----------|-------|---------------|
| Formato | **Parquet** | Columnar, comprimido nativamente, lectura selectiva de columnas, compatible Spark/Hive. Reduce tamaño ~80% vs JSON. |
| Particionado | `par=XXX/fecha=YYYY-MM-DD` | Las consultas por par son frecuentes; así solo se lee la carpeta relevante. La fecha como subnivel evita carpetas gigantes dentro de cada par. |
| Replicación | **1** | Clúster Docker local de 1 solo nodo. Replicar 3 veces no aporta tolerancia a fallos (mismo disco físico) y triplica el espacio ocupado. |

## 5. Procesamiento — Spark Streaming

| Indicador | Ventana | Slide | Justificación |
|-----------|---------|-------|---------------|
| SMA (Media Móvil Simple) | 5 min | 1 min | Suaviza ruido de segundos sin perder reactividad. Adecuado para BTC que puede moverse 3% en minutos. |
| Variación % entre ticks | — | Cada tick | Detección inmediata de movimientos bruscos. |

- **Tipo de ventana:** Sliding window (solapada) para tener actualizaciones frecuentes sin esperar a que cierre la ventana.
- **Salida histórica:** Escritura directa a HDFS en Parquet.
- **Salida tiempo real:** Métricas expuestas vía `prometheus_client` para Grafana.

## 6. Observabilidad — Prometheus + Grafana

### Capa de Infraestructura
- **Node Exporter:** CPU, RAM, disco, red del host.
- **cAdvisor:** Métricas por contenedor Docker.

### Capa de Negocio (mínimo 3 métricas propias)
1. `crypto_precio_actual{par}` (Gauge): Último precio recibido.
2. `pipeline_mensajes_total` (Counter): Acumulado de mensajes procesados por Spark.
3. `kafka_consumer_lag` (Gauge): Diferencia entre offset último y consumido.

### Alertas (mínimo 1)
- **Lag crítico:** Si `kafka_consumer_lag > 1000` durante más de 2 minutos → severidad crítica.
  - *Justificación del umbral:* 1000 mensajes en `miniTicker` ~16 minutos de retraso (1 msg/s × 2 pares). Más allá de eso, el dashboard pierde utilidad en tiempo real.

## 7. Productor Python (Binance)

| Aspecto | Decisión | Justificación |
|---------|----------|---------------|
| Librería WS | `websocket-client` | Ligera, manejo de eventos con callbacks, compatible con reconexión manual. |
| Combined Stream | `/stream?streams=...` | Una única conexión WS para los 4 streams (2 pares × 2 tipos). Reduce overhead de red y simplifica el código. |
| Reconexión | Backoff exponencial (1s → 60s) | Si Binance cierra la conexión o hay un corte de red, no saturamos con intentos instantáneos. La rúbrica valora la reconexión automática para nota alta. |
| Enriquecimiento de mensaje | Se añade `timestamp`, `par`, `tipo_stream` | Normaliza el formato independientemente del payload original de Binance. Facilita el particionado HDFS y el filtrado en Spark. |
| Métricas expuestas | `crypto_precio_actual`, `pipeline_mensajes_enviados_total`, `websocket_reconexiones_total` | Las 3 métricas son de negocio directo y permiten detectar: precio congelado, caída del pipeline, o inestabilidad de red. |
| Despliegue | Contenedor Docker propio (`producer`) | Cumple el requisito obligatorio de que cada componente sea un contenedor independiente en el `docker-compose.yml`. |

## 8. Procesamiento — Spark Structured Streaming

| Aspecto | Decisión | Justificación |
|---------|----------|---------------|
| API | Structured Streaming (DataFrames) | API moderna de Spark. Maneja watermark, ventanas y checkpoints de forma nativa. |
| Indicadores | SMA (ventana deslizante) + Variación % intra-ventana | Dos indicadores de dificultad básica-media que demuestran agregación temporal. SMA suaviza el ruido; variación % detecta volatilidad. |
| Ventana | 2 minutos / 30 segundos (demo local) | En producción real usaríamos 5 min / 1 min. Para la demo local aceleramos la ventana para poder validar resultados en tiempo razonable sin perder la lógica. |
| Watermark | 1 minuto | Permite tolerar datos ligeramente tardíos sin bloquear la emisión de resultados indefinidamente. En un entorno local con un solo productor, 1 min es suficiente. |
| Salida histórica | HDFS en Parquet vía `foreachBatch` | `foreachBatch` da control total sobre la escritura (modo append, particionado dinámico). |
| Particionado en escritura | `par=XXX/fecha=YYYY-MM-DD` | Spark escribe automáticamente las columnas `par` y `fecha` como carpetas. Las consultas posteriores en Hive solo leen las carpetas necesarias. |
| Checkpointing | En HDFS (`/data/cripto/_checkpoints`) | Si el contenedor de Spark se reinicia, recupera el offset de Kafka y continúa sin duplicar ni perder datos. |
| Métricas de negocio | `crypto_precio_sma`, `crypto_variacion_pct`, `pipeline_mensajes_procesados_total`, `spark_batches_procesados_total` | Métricas propias expuestas con `prometheus_client` desde el driver de Spark. Complementan las del productor y permiten monitorizar el procesamiento. |

## 9. Almacenamiento — HDFS + Hive

| Aspecto | Decisión | Justificación |
|---------|----------|---------------|
| Formato | **Parquet** | Columnar, comprimido nativamente (snappy), compatible 100% con Spark/Hive. Reduce tamaño ~80% vs JSON/CSV. |
| Particionado | `par=XXX/fecha=YYYY-MM-DD` | Filtrado eficiente: una query por par solo lee su carpeta sin escanear todo el histórico. La fecha como subcarpeta evita archivos gigantes dentro de cada par. |
| Replicación HDFS | **1** | Clúster Docker local de 1 solo nodo físico. Replicar 3 veces no aporta tolerancia a fallos (mismo disco) y triplica el espacio. Se justifica en la memoria como decisión consciente de ahorro de recursos en desarrollo. |
| Checkpointing Spark | En HDFS (`/data/cripto/_checkpoints`) | Garantiza exactly-once processing. Si el job de streaming se reinicia, recupera el offset desde Kafka sin duplicar datos. |
| Tablas Hive | **Externas** (`EXTERNAL TABLE`) | Los datos los genera y controla Spark. Hive solo apunta a ellos. Si se borra la tabla, los datos Parquet permanecen en HDFS. |
| Motor de consulta | Hive para DDL + Spark SQL para agregaciones | Hive 4.0 en contenedor Docker local tiene limitaciones con Hive-on-MR para agregaciones complejas. Spark SQL lee nativamente los Parquet de HDFS y ejecuta las queries analíticas de forma eficiente. Esto es técnicamente sólido y justificable en la memoria. |

## 10. Observabilidad — Prometheus + Grafana

| Aspecto | Decisión | Justificación |
|---------|----------|---------------|
| Modelo de scrapeo | **Pull** (Prometheus pregunta periódicamente) | Estándar de facto. Más fiable que push en entornos efímeros de contenedores. |
| Exporters infraestructura | Node Exporter + cAdvisor | Node Exporter cubre CPU/RAM/disco/red del host. cAdvisor cubre métricas por contenedor Docker (CPU, memoria, red, filesystem). |
| Métricas de negocio | 6 métricas propias expuestas con `prometheus_client` | Producer: precio actual, mensajes enviados, reconexiones. Spark: SMA, variación %, mensajes procesados, batches. |
| Dashboards separados | **2 dashboards diferenciados** | Infraestructura (node/cAdvisor) y Negocio (precios, indicadores, throughput). La rúbrica valora positivamente la separación por capas. |
| Alertas configuradas | 3 alertas con umbral y severidad | `ProducerDown` (critical), `NoMessagesProcessed` (warning), `HighPriceVariation` (critical). Cada una con duración `for` justificada. |
| Refresh de dashboards | 5 segundos | Equilibrio entre reactividad y carga del navegador. |

## 11. Docker Compose — Imágenes Seleccionadas

| Servicio | Imagen | Motivo |
|----------|--------|--------|
| Kafka | `confluentinc/cp-kafka:7.5.0` | Estándar de la industria, documentación extensa, estable. |
| Zookeeper | `confluentinc/cp-zookeeper:7.5.0` | Par oficial de Confluent. |
| HDFS | `bde2020/hadoop-namenode/datanode:2.0.0-hadoop3.2.1-java8` | Imágenes educativas probadas, configuración mínima. |
| Spark | `bitnami/spark:3.5` | Ligera, modo master/worker sencillo. |
| Prometheus | `prom/prometheus:v2.50` | Oficial. |
| Grafana | `grafana/grafana:10.3` | Oficial. |
| Node Exporter | `prom/node-exporter:v1.7` | Oficial. |
| cAdvisor | `gcr.io/cadvisor/cadvisor:v0.47.2` | Oficial Google. |

**Nota sobre puertos:** Spark Master UI usa 8080 internamente; cAdvisor se mapea al host en 8081 para evitar conflicto.
