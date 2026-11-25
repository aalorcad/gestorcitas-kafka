#!/usr/bin/env bash
# Crea los tópicos del GestorCitas con su configuración (idempotente: --if-not-exists).
# Se ejecuta dentro del contenedor kafka-init (docker-compose.yml), que define BOOTSTRAP.
#
# | Tópico                              | Part. | RF | Limpieza | Retención | Clave     |
# | gestorcitas.citas.eventos           | 3     | 3  | delete   | 7 días    | citaId    |
# | gestorcitas.usuarios.estado         | 3     | 3  | compact  | último por clave; tombstones 1 día | usuarioId |
# | gestorcitas.citas.eventos.DLT       | 3     | 3  | delete   | 14 días   | -         |
# | gestorcitas.usuarios.estado.DLT     | 3     | 3  | delete   | 14 días   | -         |
# Cada DLT tiene las mismas particiones que su tópico origen (el recuperador publica en la misma partición).
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka1:29092,kafka2:29092,kafka3:29092}"
SIETE_DIAS=604800000
CATORCE_DIAS=1209600000
UN_DIA=86400000

crear() {
  local nombre="$1" particiones="$2"; shift 2
  echo ">> $nombre ($particiones particiones, RF 3)"
  kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$nombre" --partitions "$particiones" --replication-factor 3 \
    --config min.insync.replicas=2 "$@"
}

crear gestorcitas.citas.eventos        3 --config cleanup.policy=delete  --config retention.ms=$SIETE_DIAS
crear gestorcitas.usuarios.estado      3 --config cleanup.policy=compact --config min.cleanable.dirty.ratio=0.1 --config segment.ms=3600000 --config delete.retention.ms=$UN_DIA
crear gestorcitas.citas.eventos.DLT    3 --config cleanup.policy=delete  --config retention.ms=$CATORCE_DIAS
crear gestorcitas.usuarios.estado.DLT  3 --config cleanup.policy=delete  --config retention.ms=$CATORCE_DIAS

echo
echo "== Tópicos del clúster =="
kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topics-with-overrides
