import datetime
import json
import logging
import os
import re
import sys
import threading
import time
import uuid
from os import environ

from bson import json_util
from flask import Flask, render_template, request
from flask_socketio import SocketIO, join_room, leave_room
import iso8601
import joblib
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from pyelasticsearch import ElasticSearch

import config
import predict_utils

# ==============================================================================
# 1. CONFIGURATION & CONSTANTS
# ==============================================================================
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
REQUEST_TOPIC = "flight-delay-ml-request"
RESPONSE_TOPIC = "flight-delay-ml-response"

# ==============================================================================
# 2. APPLICATION INITIALIZATION (Flask, Logging, DBs, Kafka)
# ==============================================================================
app = Flask(__name__)

# Configure centralized logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize WebSockets
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading"
)

# Initialize Databases & External Services
client = MongoClient()
elastic = ElasticSearch(config.ELASTIC_URL)

# Initialize Kafka Producer (Moved after KAFKA_BOOTSTRAP_SERVERS definition)
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

# ==============================================================================
# 3. FLASK HTTP ROUTES
# ==============================================================================
@app.route("/flights/delays/predict_kafka")
def flight_delays_page_kafka():
    """Serves flight delay prediction page with polling form"""
    form_config = [
        {'field': 'DepDelay', 'label': 'Departure Delay', 'value': 5},
        {'field': 'Carrier', 'value': 'AA'},
        {'field': 'FlightDate', 'label': 'Date', 'value': '2016-12-25'},
        {'field': 'Origin', 'value': 'ATL'},
        {'field': 'Dest', 'label': 'Destination', 'value': 'SFO'}
    ]
    return render_template('flight_delays_predict_kafka.html', form_config=form_config)


@app.route("/flights/delays/predict/classify_realtime", methods=['POST'])
def classify_flight_delays_realtime():
    """POST API for fetching form data, querying Cassandra, and publishing to Kafka"""
    
    # Define the form fields and their targeted types
    api_field_type_map = {
        "DepDelay": float,
        "Carrier": str,
        "FlightDate": str,
        "Dest": str,
        "FlightNum": str,
        "Origin": str
    }

    # Fetch values directly into the feature dict
    prediction_features = {}
    for field_name, field_type in api_field_type_map.items():
        prediction_features[field_name] = request.form.get(field_name, type=field_type)
    
    # Fetch derived distance from Cassandra
    prediction_features['Distance'] = predict_utils.get_flight_distance_cassandra(
        client, 
        prediction_features['Origin'],
        prediction_features['Dest']
    )
    
    # Extract date features (DayOfYear, DayOfMonth, DayOfWeek)
    date_features_dict = predict_utils.get_regression_date_args(
        prediction_features['FlightDate']
    )
    for key, value in date_features_dict.items():
        prediction_features[key] = value
    
    # Add metadata (Timestamp and Unique ID)
    prediction_features['Timestamp'] = predict_utils.get_current_timestamp()
    unique_id = str(uuid.uuid4())
    prediction_features["UUID"] = unique_id

    # Log incoming request payload
    logger.info("=== NEW PREDICTION REQUEST RECEIVED ===")
    logger.info(f"Generated UUID: {unique_id}")
    logger.info(f"Payload Features: {prediction_features}")

    # Publish message to Kafka Request Topic (Removed duplicate producer.send)
    producer.send(REQUEST_TOPIC, prediction_features)
    producer.flush()
    logger.info(f"[KAFKA PRODUCER] Message successfully published to topic: {REQUEST_TOPIC}")

    response = {
        "status": "OK",
        "id": unique_id
    }
    return json.dumps(response)

# ==============================================================================
# 4. WEBSOCKET EVENTS
# ==============================================================================
@socketio.on('register')
def handle_register(data):
    """Registers a specific client web session to a Socket room named after their UUID"""
    client_uuid = data.get('uuid')
    if client_uuid:
        join_room(client_uuid)
        logger.info("=== SOCKET ROOM REGISTER ===")
        logger.info(f"[SOCKET ID] {request.sid}")
        logger.info(f"[ROOM UUID] {client_uuid}")
        logger.info("[WEBSOCKET] Client successfully joined dedicated room.")
    else:
        logger.warning(f"[WEBSOCKET] Registration failed. No UUID provided by SID: {request.sid}")

# ==============================================================================
# 5. BACKGROUND KAFKA CONSUMER (Kafka -> WebSockets)
# ==============================================================================
def listen_kafka_response_topic():
    """Background worker loop consuming ML responses safely from Kafka"""
    while True:
        try:
            logger.info(f"[KAFKA CONSUMER] Connecting to response topic: {RESPONSE_TOPIC}...")
            
            # ELIMINAMOS el value_deserializer de aquí para recibir bytes puros y controlarlos
            consumer = KafkaConsumer(
                RESPONSE_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                group_id="flask-websocket-consumer",
                auto_offset_reset="latest",
                enable_auto_commit=True
            )

            logger.info(f"[KAFKA CONSUMER] Listening for messages on: {RESPONSE_TOPIC}")

            for record in consumer:
                # Intentamos decodificar el mensaje de manera segura
                try:
                    raw_value = record.value.decode("utf-8")
                    message = json.loads(raw_value)
                except Exception as json_err:
                    # Si el mensaje no es JSON, lo saltamos y evitamos que rompa el consumidor
                    logger.error("=== INVALID MESSAGE FORMAT ===")
                    logger.error(f"[SKIPPED] Message is not a valid JSON. Raw content: {record.value}")
                    logger.error(f"[REASON] {str(json_err)}")
                    continue # Salta al siguiente mensaje sin caerse

                target_uuid = message.get("UUID")
                
                logger.info("=== KAFKA RESPONSE RECEIVED ===")
                logger.info(f"[PAYLOAD] {json.dumps(message, indent=2)}")
                logger.info(f"[TARGET ROOM] Routing to targeted room UUID: {target_uuid}")

                if target_uuid:
                    socketio.emit(
                        "kafka_response",
                        message,
                        room=target_uuid
                    )
                    logger.info("[WEBSOCKET] Event successfully emitted to targeted client room.")
                else:
                    logger.error("[WEBSOCKET] Missing UUID in Kafka message. Cannot route response.")

        except Exception as e:
            # Este bloque solo se ejecutará si se cae la conexión de red con Kafka
            logger.error("=== KAFKA CONNECTION ERROR ===")
            logger.error(f"[ERROR DETAILS] {str(e)}")
            logger.info("[ACTION] Reconnecting to Kafka Broker in 5 seconds...")
            time.sleep(5)
# ==============================================================================
# 6. APPLICATION RUNNER
# ==============================================================================
if __name__ == "__main__":
    # Start the Kafka consumer thread in the background
    kafka_thread = threading.Thread(target=listen_kafka_response_topic)
    kafka_thread.daemon = True
    kafka_thread.start()

    # Start Flask-SocketIO Server
    socketio.run(
        app,
        host="0.0.0.0",
        port=5001,
        debug=True,
        use_reloader=False,
        allow_unsafe_werkzeug=True
    )