# Flight Delay Prediction Pipeline

Sistema cloud-native para predicción de retrasos de vuelos en tiempo real, construido sobre una arquitectura de data lakehouse con procesamiento distribuido en Kubernetes.

## Tech Stack

- **Procesamiento:** Apache Spark 4.1.1 (PySpark + Scala)
- **Formato de tabla:** Apache Iceberg 1.10.0
- **Streaming:** Apache Kafka 4.2.0 (KRaft)
- **Almacenamiento:** MinIO (S3-compatible)
- **Base de datos:** Apache Cassandra 4.1
- **ML Tracking:** MLflow 2.13.0
- **Backend:** Python Flask
- **Orquestacion:** Kubernetes (GKE), Kustomize
- **Imagenes:** Docker Hub (hineill/ibdn-*)

## Estructura del Repositorio

| Directorio | Descripcion |
| :--- | :--- |
| `docker/` | Dockerfiles de cada componente |
| `data/` | Dataset de vuelos y distancias origen-destino |
| `k8s/base/` | Namespace, ConfigMaps, Secrets, PVCs |
| `k8s/infra/` | Kafka, Cassandra, MinIO, MLflow, Jobs de init |
| `k8s/spark/` | Spark master, workers, train job, predictor, cronjob |
| `k8s/app/` | Flask web |
| `resources/cassandra-init/` | Script de importacion de distancias |
| `resources/flask-web/` | Aplicacion web Flask |
| `resources/spark-pyspark-iceberg/` | Script de conversion a formato Iceberg |
| `resources/spark-pyspark-train-model/` | Script de entrenamiento del modelo |
| `resources/spark-scala-prediction/` | Predictor en tiempo real (Scala) |

## Arquitectura

El pipeline sigue un flujo de datos secuencial dividido en cinco fases:

1. **Inicializacion** — los Jobs de init crean los buckets en MinIO, el keyspace en Cassandra, los topics en Kafka y convierten el dataset raw a formato Iceberg.
2. **Entrenamiento** — Spark lee la tabla Iceberg, entrena un modelo RandomForest con PySpark MLlib y guarda el modelo en `models/production/` registrando metricas en MLflow.
3. **Prediccion** — el predictor Scala consume requests de Kafka, aplica el modelo y escribe las predicciones en Cassandra.
4. **Serving** — Flask sirve la UI, publica requests en Kafka y muestra las predicciones desde Cassandra.
5. **Reentrenamiento** — un CronJob reentrena el modelo cada 20 minutos con los datos mas recientes en Iceberg.

## Componentes y Servicios

| Servicio | Tipo K8s | Exposicion |
| :--- | :--- | :--- |
| Kafka | Deployment | ClusterIP (interno) |
| Cassandra | Deployment | ClusterIP (interno) |
| MinIO API | Deployment | ClusterIP (interno) |
| MinIO Console | Service | LoadBalancer :9001 |
| MLflow | Deployment | LoadBalancer :5000 |
| Spark Master | Deployment | ClusterIP (interno) |
| Spark Master UI | Service | LoadBalancer :8080 |
| Flask Web | Deployment | LoadBalancer :80 |

## Requisitos previos

- Cluster GKE Standard con minimo 3 nodos `e2-standard-4`
- `kubectl` configurado apuntando al cluster
- Imagenes publicadas en Docker Hub

## Despliegue

### 1. Conectar al cluster

```bash
gcloud container clusters get-credentials ibdn-cluster \
  --zone europe-southwest1-a \
  --project <PROJECT_ID>
```

### 2. Desplegar todo

```bash
kubectl apply -k k8s/
```

El orden de arranque es gestionado automaticamente por initContainers. No es necesario aplicar los manifiestos en orden especifico.

### 3. Verificar el estado

```bash
# Estado de todos los pods
kubectl get pods -n ibdn

# IPs publicas de los servicios
kubectl get svc -n ibdn

# Recursos del cluster
kubectl top nodes
kubectl top pods -n ibdn
```

### 4. Verificar el pipeline completo

```bash
# Topics de Kafka
kubectl exec -n ibdn $(kubectl get pod -n ibdn -l app=kafka \
  -o jsonpath='{.items[0].metadata.name}') -- \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Predicciones en Cassandra
kubectl exec -n ibdn $(kubectl get pod -n ibdn -l app=cassandra \
  -o jsonpath='{.items[0].metadata.name}') -- \
  cqlsh -e "SELECT count(*) FROM agile_data_science.flight_delay_predictions;"
```

## Reentrenamiento automatico

El modelo se reentrena automaticamente cada 20 minutos mediante un CronJob. El nuevo modelo sobreescribe `models/production/` y se registra en MLflow con timestamp.

```bash
# Ver historial de ejecuciones
kubectl describe cronjob spark-retrain -n ibdn

# Forzar reentrenamiento manual
kubectl create job retrain-manual --from=cronjob/spark-retrain -n ibdn
```

## Imagenes Docker

```
hineill/ibdn-minio-init:latest
hineill/ibdn-cassandra-init:latest
hineill/ibdn-iceberg-init:latest
hineill/ibdn-spark-train-model:latest
hineill/ibdn-spark-flight-predictor:latest
hineill/ibdn-flask-web:latest
```

Para rebuildar y publicar:

```bash
bash push-images.sh
```