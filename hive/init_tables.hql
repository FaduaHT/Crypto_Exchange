-- ============================================================
-- Tablas externas Hive sobre datos Parquet en HDFS
-- ============================================================

-- Tabla para datos enriquecidos de miniTicker (con indicadores)
CREATE EXTERNAL TABLE IF NOT EXISTS cripto_mini (
  par STRING,
  fecha STRING,
  sma_5min DOUBLE,
  variacion_pct DOUBLE,
  max_precio DOUBLE,
  min_precio DOUBLE
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/cripto/miniTicker';

-- Tabla para velas (kline) en bruto
CREATE EXTERNAL TABLE IF NOT EXISTS cripto_kline (
  par STRING,
  fecha STRING,
  event_timestamp DOUBLE,
  tipo_stream STRING,
  datos MAP<STRING,STRING>
)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/cripto/kline';

-- Consultas de validación
SELECT 'Total registros miniTicker' AS metrica, COUNT(*) AS valor FROM cripto_mini;
SELECT 'Total registros kline' AS metrica, COUNT(*) AS valor FROM cripto_kline;
SELECT par, AVG(sma_5min) AS avg_sma, AVG(variacion_pct) AS avg_var FROM cripto_mini GROUP BY par;
SELECT par, COUNT(*) AS num_velas FROM cripto_kline GROUP BY par;
