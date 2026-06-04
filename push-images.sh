#!/bin/bash
# push-images.sh
# Script para pushear las imágenes personalizadas a Docker Hub
# Usuario: hineill

DOCKER_USER="hineill"
PROJECT="ibdn-project"

echo "=== Limpiando imágenes huérfanas ==="
docker image prune -f

echo ""
echo "=== Logging in to Docker Hub ==="
docker login

echo ""
echo "=== Tagging and pushing images ==="

# minio-init
echo "→ minio-init"
docker tag ${PROJECT}-minio-init:latest ${DOCKER_USER}/ibdn-minio-init:latest
docker push ${DOCKER_USER}/ibdn-minio-init:latest

# cassandra-init
echo "→ cassandra-init"
docker tag ${PROJECT}-cassandra-init:latest ${DOCKER_USER}/ibdn-cassandra-init:latest
docker push ${DOCKER_USER}/ibdn-cassandra-init:latest

# iceberg-init
echo "→ iceberg-init"
docker tag ${PROJECT}-iceberg-init:latest ${DOCKER_USER}/ibdn-iceberg-init:latest
docker push ${DOCKER_USER}/ibdn-iceberg-init:latest

# spark-train-model
echo "→ spark-train-model"
docker tag ${PROJECT}-spark-train-model:latest ${DOCKER_USER}/ibdn-spark-train-model:latest
docker push ${DOCKER_USER}/ibdn-spark-train-model:latest

# spark-flight-predictor
echo "→ spark-flight-predictor"
docker tag ${PROJECT}-spark-flight-predictor:latest ${DOCKER_USER}/ibdn-spark-flight-predictor:latest
docker push ${DOCKER_USER}/ibdn-spark-flight-predictor:latest

# flask-web
echo "→ flask-web"
docker tag ${PROJECT}-flask-web:latest ${DOCKER_USER}/ibdn-flask-web:latest
docker push ${DOCKER_USER}/ibdn-flask-web:latest

echo ""
echo "=== Done! Imágenes en Docker Hub ==="
echo "  ${DOCKER_USER}/ibdn-minio-init:latest"
echo "  ${DOCKER_USER}/ibdn-cassandra-init:latest"
echo "  ${DOCKER_USER}/ibdn-iceberg-init:latest"
echo "  ${DOCKER_USER}/ibdn-spark-train-model:latest"
echo "  ${DOCKER_USER}/ibdn-spark-flight-predictor:latest"
echo "  ${DOCKER_USER}/ibdn-flask-web:latest"