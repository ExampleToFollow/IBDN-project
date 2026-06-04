# config.py, a configuration file for index.py
import os
RECORDS_PER_PAGE=15
AIRPLANE_RECORDS_PER_PAGE=5
ELASTIC_URL='http://localhost:9200/agile_data_science'
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
PREDICTION_TOPIC = os.getenv("PREDICTION_TOPIC", "flight-delay-ml-request")
RESPONSE_TOPIC = os.getenv("RESPONSE_TOPIC", "flight-delay-ml-response")