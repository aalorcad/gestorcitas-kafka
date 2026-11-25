# gestorcitas-kafka

Plataforma de **streaming Apache Kafka** de GestorCitaCloudNative: ensemble de **3 Zookeeper**, **3 brokers** y
**Kafka UI**, levantados con Docker Compose. Los productores y consumidores están en el repositorio **gestorcitas-backend**.

## Contenido

| Archivo | Uso |
|---|---|
| `docker-compose.yml` | 3 Zookeeper + 3 brokers + `kafka-init` (crea los tópicos) + Kafka UI. Imágenes con versión fija |
| `scripts/topicos.sh` | Definición de los tópicos (particiones, réplicas, limpieza y retención). Idempotente |
| `.env.example` | Variables necesarias (copiar como `.env`, que no se sube a Git) |

## Arquitectura

| Servicio | Imagen | Rol |
|---|---|---|
| `zookeeper1..3` | `confluentinc/cp-zookeeper:7.6.1` | Ensemble con quórum de 2: guarda los metadatos del clúster (brokers vivos, controlador, líderes, configuración de tópicos). Con 1 nodo caído el quórum se mantiene |
| `kafka1..3` | `confluentinc/cp-kafka:7.6.1` | Guardan el log particionado y replicado. Uno de ellos es el **controlador** (elige líderes de partición) |
| `kafka-init` | `confluentinc/cp-kafka:7.6.1` | Tarea única: ejecuta `scripts/topicos.sh` y termina |
| `kafka-ui` | `provectuslabs/kafka-ui:v0.7.2` | Administración y monitoreo: brokers, tópicos, mensajes, grupos, offsets y lag |

**Durabilidad**: factor de replicación 3 (también en `__consumer_offsets`), `min.insync.replicas=2` y productores con
`acks=all`: cada evento se confirma cuando está en al menos 2 brokers. Con un broker caído el clúster sigue
aceptando escrituras y lecturas; al volver, su réplica se pone al día y reingresa al ISR.
`auto.create.topics.enable=false`: solo existen los tópicos definidos explícitamente.

### Listeners

| Listener | Dirección | Quién lo usa |
|---|---|---|
| `INTERNAL` | `kafkaN:29092` | Replicación entre brokers, Kafka UI, `kafka-init` y los microservicios en la red Docker `gestorcitas-kafka` (local) |
| `EXTERNAL` | `${KAFKA_ADVERTISED_HOST}:9092 / 9093 / 9094` | Clientes fuera de Docker: tu Mac (`localhost`) o los microservicios de otra EC2 (IP privada de esta) |

## Tópicos

| Tópico | Part. | RF | Limpieza | Retención | Clave | Propósito |
|---|---|---|---|---|---|---|
| `gestorcitas.citas.eventos` | 3 | 3 | `delete` | 7 días (`retention.ms=604800000`) | `citaId` | Historial de cambios de estado de cada cita. Se puede releer para reprocesar o recalcular indicadores |
| `gestorcitas.usuarios.estado` | 3 | 3 | `compact` | Último registro por clave; tombstones 1 día (`delete.retention.ms`) | `usuarioId` | Estado actual de cada usuario: un consumidor nuevo reconstruye la tabla completa leyendo desde el inicio |
| `gestorcitas.citas.eventos.DLT` | 3 | 3 | `delete` | 14 días | — | Registros que fallaron (inválidos o con error persistente), para investigarlos |
| `gestorcitas.usuarios.estado.DLT` | 3 | 3 | `delete` | 14 días | — | Ídem para el tópico de usuarios |

- **3 particiones**: hasta 3 consumidores en paralelo por grupo; la clave asegura que todos los eventos de una misma
  cita o usuario caigan en la misma partición y se lean **en orden**.
- **3 réplicas**: una copia en cada broker (líder + 2 seguidores).
- Cada `.DLT` tiene las mismas particiones que su origen: el registro fallido conserva su partición.

### Grupos de consumidores

| Grupo | Servicio (repo backend) | Tópicos | Uso |
|---|---|---|---|
| `analitica` | `ms-analitica` (2 instancias) | `citas.eventos`, `usuarios.estado` | Indicadores en tiempo real (reservas por especialidad/médico, cancelaciones, usuarios) |
| `historial-citas` | `ms-notificaciones` | `citas.eventos` | Historial de estados de cada cita |

Ambos grupos leen **todos** los eventos de `citas.eventos` con offsets independientes.

## Levantar

```bash
cp .env.example .env               # KAFKA_ADVERTISED_HOST y clave de Kafka UI
docker compose up -d
docker compose ps                  # 3 zookeeper, 3 kafka, kafka-ui (kafka-init termina con código 0)
docker logs gestorcitas-kafka-init # tópicos creados
```
Kafka UI: `http://<host>:8090`.

En AWS (EC2 `gestorcitas-kafka`, t3.medium): `KAFKA_ADVERTISED_HOST=<IP privada de la EC2>`.
Security Group: 9092-9094 desde `gestorcitas-ms-sg`, 8090 y 22 desde tu IP.

## Comandos de supervisión (CLI)

```bash
# Tópicos, líderes, réplicas e ISR
docker exec gestorcitas-kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic gestorcitas.citas.eventos
# Offsets, lag y asignación de particiones por grupo
docker exec gestorcitas-kafka1 kafka-consumer-groups --bootstrap-server kafka1:29092 --describe --group analitica
# Configuración (retención) de un tópico
docker exec gestorcitas-kafka1 kafka-configs --bootstrap-server kafka1:29092 --describe --entity-type topics --entity-name gestorcitas.citas.eventos
```

## Pruebas de tolerancia a fallos

```bash
docker stop gestorcitas-kafka2     # se reelige líder; productores y consumidores siguen operando (ISR = 2)
docker start gestorcitas-kafka2    # la réplica se sincroniza y vuelve al ISR
```
