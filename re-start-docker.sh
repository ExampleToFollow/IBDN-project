#!/usr/bin/env bash
#El objetivo de este script es levantar todo el ambiente de cero en docker compose xd 
set -e
echo "=== [1/4] Parando containers y eliminando volúmenes ==="
docker compose down -v --remove-orphans
echo "=== [2/4] Eliminando imágenes del proyecto ==="
docker compose down --rmi all
echo "=== [3/4] Limpiando capas cacheadas y recursos huérfanos ==="
docker system prune -a --volumes -f
echo "=== [4/4] Reconstruyendo y levantando todo desde cero ==="
docker compose build --no-cache
docker compose up
