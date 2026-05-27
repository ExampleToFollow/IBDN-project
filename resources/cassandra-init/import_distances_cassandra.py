import json
import urllib.request
from cassandra.cluster import Cluster

DATA_URL = "http://s3.amazonaws.com/agile_data_science/origin_dest_distances.jsonl"

print("Descargando datos...")
response = urllib.request.urlopen(DATA_URL)

print("Conectando a Cassandra...")
cluster = Cluster(["cassandra"], port=9042)
session = cluster.connect()

session.execute("CREATE KEYSPACE IF NOT EXISTS agile_data_science WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")
session.set_keyspace("agile_data_science")

session.execute("""
CREATE TABLE IF NOT EXISTS flight_distances (
    origin text, dest text, distance double, PRIMARY KEY ((origin), dest)
)""")

session.execute("""
CREATE TABLE IF NOT EXISTS flight_delay_predictions (
    uuid text PRIMARY KEY,
    origin text,
    dest text,
    carrier text,
    flight_date date,
    day_of_week int,
    day_of_month int,
    day_of_year int,
    dep_delay double,
    distance double,
    route text,
    prediction text
    )""")

stmt = session.prepare("INSERT INTO flight_distances (origin, dest, distance) VALUES (?, ?, ?)")
count = 0

for line in response:
    row = json.loads(line.decode("utf-8"))
    if row.get("Origin") and row.get("Dest") and row.get("Distance") is not None:
        session.execute(stmt, (row["Origin"], row["Dest"], float(row["Distance"])))
        count += 1

print(f"Distancias cargadas: {count}")
cluster.shutdown()