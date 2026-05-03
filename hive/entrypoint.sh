#!/bin/bash
set -e

# Inicializar schema (idempotente)
/opt/hive/bin/schematool -dbType derby -initOrUpgradeSchema || true

# Arrancar HiveServer2
exec /opt/hive/bin/hiveserver2
