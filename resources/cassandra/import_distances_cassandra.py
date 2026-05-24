import json
import time
from cassandra.cluster import Cluster

DATA_PATH = "/data/origin_dest_distances.jsonl"

print("Conectando a Cassandra...")

cluster = Cluster(["cassandra"], port=9042)
session = cluster.connect()

session.execute("""
CREATE KEYSPACE IF NOT EXISTS agile_data_science
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

session.set_keyspace("agile_data_science")

session.execute("""
CREATE TABLE IF NOT EXISTS flight_distances (
    origin text,
    dest text,
    distance double,
    PRIMARY KEY ((origin), dest)
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS flight_delay_predictions (
    uuid text PRIMARY KEY,
    origin text,
    dest text,
    carrier text,
    flight_num text,
    flight_date text,
    dep_delay double,
    distance double,
    prediction double,
    timestamp text
)
""")

insert_stmt = session.prepare("""
INSERT INTO flight_distances (origin, dest, distance)
VALUES (?, ?, ?)
""")

count = 0

with open(DATA_PATH, "r") as f:
    for line in f:
        row = json.loads(line)

        origin = row.get("Origin")
        dest = row.get("Dest")
        distance = row.get("Distance")

        if origin and dest and distance is not None:
            session.execute(insert_stmt, (origin, dest, float(distance)))
            count += 1

print(f"Distancias cargadas: {count}")

cluster.shutdown()