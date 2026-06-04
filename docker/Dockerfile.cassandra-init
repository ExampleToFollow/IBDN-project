FROM python:3.8-slim
RUN pip install cassandra-driver -q
COPY resources/cassandra-init/import_distances_cassandra.py /app/init.py