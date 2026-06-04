# Flight Delay Prediction Pipeline

Sistema cloud-native para predicción de retrasos de vuelos en tiempo real, construido sobre una arquitectura de data lakehouse con procesamiento distribuido en Kubernetes.

## Tech Stack

| Componente | Versión | Descripción |
| :--- | :--- | :--- |
| Apache Spark | 4.1.1 | Motor de procesamiento distribuido. Ejecuta el entrenamiento del modelo (PySpark) y las predicciones en tiempo real (Scala Structured Streaming) |
| Apache Iceberg | 1.10.0 | Formato de tabla open-source sobre MinIO que actúa como Data Lakehouse. Almacena los datos de entrenamiento en formato Parquet con soporte para snapshots y time travel |
| Apache Kafka | 4.2.0 | Broker de mensajería en modo KRaft (sin Zookeeper). Gestiona los topics de requests y responses de predicciones entre Flask y el predictor Spark |
| MinIO | RELEASE.2025 | Almacenamiento objeto S3-compatible. Contiene el dataset raw, la tabla Iceberg, los modelos entrenados y los artefactos de MLflow |
| Apache Cassandra | 4.1 | Base de datos NoSQL distribuida. Almacena las distancias origen-destino y las predicciones generadas |
| MLflow | 2.13.0 | Plataforma de tracking de experimentos ML. Registra métricas, parámetros y versiones del modelo en cada entrenamiento |
| Python Flask | - | Servidor web que sirve la interfaz de usuario, publica requests en Kafka y muestra predicciones en tiempo real via websockets |
| Kubernetes (GKE) | - | Orquestador de contenedores. Gestiona el ciclo de vida de todos los servicios, la persistencia de datos y el reentrenamiento automático mediante CronJob |

## Estructura del Repositorio

| Directorio | Contenido |
| :--- | :--- |
| `docker/` | Dockerfiles de cada componente |
| `data/` | Dataset de vuelos y distancias origen-destino |
| `k8s/base/` | Namespace, ConfigMaps, Secrets, PVCs |
| `k8s/infra/` | Manifiestos de Kafka, Cassandra, MinIO, MLflow y Jobs de inicialización |
| `k8s/spark/` | Spark master, workers, job de entrenamiento, predictor y cronjob |
| `k8s/app/` | Flask web |
| `resources/cassandra-init/` | Importación de distancias origen-destino a Cassandra |
| `resources/flask-web/` | Aplicación web Flask con websockets |
| `resources/spark-pyspark-iceberg/` | Conversión del dataset raw a tabla Iceberg |
| `resources/spark-pyspark-train-model/` | Entrenamiento del modelo RandomForest con MLflow |
| `resources/spark-scala-prediction/` | Predictor en tiempo real con Spark Structured Streaming |

## Servicios expuestos

| Servicio | Puerto | Descripción |
| :--- | :--- | :--- |
| Flask Web | :80 | Interfaz principal de la aplicación |
| MinIO Console | :9001 | Explorador de buckets y objetos |
| MLflow UI | :5000 | Seguimiento de experimentos y modelos |
| Spark Master UI | :8080 | Estado del cluster y aplicaciones Spark |

## Despliegue

### 1. Conectar al cluster de gcloud 

```bash
gcloud container clusters get-credentials ibdn-cluster --zone <ZONE_ID>  --project <PROJECT_ID>
```

### 2. Desplegar todo

```bash
kubectl apply -k k8s/
```

El orden de arranque es gestionado automáticamente por initContainers — no es necesario aplicar los manifiestos en ningún orden específico.
