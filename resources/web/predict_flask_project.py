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
# CONFIGURATION
# ==============================================================================
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
REQUEST_TOPIC = "flight-delay-ml-request"
RESPONSE_TOPIC = "flight-delay-ml-response"

REQUIRED_FIELDS = {
    "DepDelay": float,
    "Carrier": str,
    "FlightDate": str,
    "Dest": str,
    "FlightNum": str,
    "Origin": str
}


# ==============================================================================
# APP INITIALIZATION
# ==============================================================================
app = Flask(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s"
)

logger = logging.getLogger(__name__)

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading"
)

client = MongoClient()

producer = None


# ==============================================================================
# KAFKA PRODUCER
# ==============================================================================
def get_kafka_producer():
    global producer

    if producer is None:
        logger.info("[KAFKA PRODUCER] Creating Kafka producer...")

        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda value: json.dumps(value).encode("utf-8")
        )

        logger.info("[KAFKA PRODUCER] Producer created successfully.")

    return producer


# ==============================================================================
# FLASK ROUTES
# ==============================================================================
@app.route("/flights/delays/predict_kafka")
def flight_delays_page_kafka():
    form_config = [
        {"field": "DepDelay", "label": "Departure Delay", "value": 5},
        {"field": "Carrier", "value": "AA"},
        {"field": "FlightDate", "label": "Date", "value": "2016-12-25"},
        {"field": "Origin", "value": "ATL"},
        {"field": "Dest", "label": "Destination", "value": "SFO"},
        {"field": "FlightNum", "label": "Flight Number", "value": "100"}
    ]

    return render_template(
        "flight_delays_predict_kafka.html",
        form_config=form_config
    )


@app.route("/flights/delays/predict/classify_realtime", methods=["POST"])
def classify_flight_delays_realtime():
    try:
        prediction_features = {}

        for field_name, field_type in REQUIRED_FIELDS.items():
            value = request.form.get(field_name, type=field_type)

            if value is None or value == "":
                logger.warning(f"[REQUEST ERROR] Missing field: {field_name}")

                return json.dumps({
                    "status": "ERROR",
                    "message": f"Missing required field: {field_name}"
                }), 400

            prediction_features[field_name] = value

        prediction_features["Distance"] = predict_utils.get_flight_distance_cassandra(
            client,
            prediction_features["Origin"],
            prediction_features["Dest"]
        )

        date_features = predict_utils.get_regression_date_args(
            prediction_features["FlightDate"]
        )

        prediction_features.update(date_features)

        request_uuid = str(uuid.uuid4())

        prediction_features["Timestamp"] = predict_utils.get_current_timestamp()
        prediction_features["UUID"] = request_uuid

        logger.info("=== NEW PREDICTION REQUEST ===")
        logger.info(f"[UUID] {request_uuid}")
        logger.info(f"[ORIGIN] {prediction_features['Origin']}")
        logger.info(f"[DEST] {prediction_features['Dest']}")
        logger.info(f"[FLIGHT DATE] {prediction_features['FlightDate']}")
        logger.info(f"[REQUEST TOPIC] {REQUEST_TOPIC}")
        logger.info(f"[PAYLOAD] {json.dumps(prediction_features)}")

        kafka_producer = get_kafka_producer()
        kafka_producer.send(REQUEST_TOPIC, prediction_features)
        kafka_producer.flush()

        logger.info("[KAFKA PRODUCER] Message sent successfully.")

        return json.dumps({
            "status": "OK",
            "id": request_uuid
        })

    except Exception as error:
        logger.exception("=== ERROR PROCESSING PREDICTION REQUEST ===")

        return json.dumps({
            "status": "ERROR",
            "message": str(error)
        }), 500


# ==============================================================================
# WEBSOCKET EVENTS
# ==============================================================================
@socketio.on("register")
def handle_register(data):
    client_uuid = data.get("UUID") if data else None

    if not client_uuid:
        logger.warning(f"[WEBSOCKET] Register failed. Missing UUID. SID={request.sid}")
        return

    join_room(client_uuid)

    logger.info("=== SOCKET ROOM REGISTERED ===")
    logger.info(f"[SOCKET ID] {request.sid}")
    logger.info(f"[ROOM UUID] {client_uuid}")


# ==============================================================================
# KAFKA CONSUMER: RESPONSE TOPIC -> WEBSOCKET
# ==============================================================================
def listen_kafka_response_topic():
    while True:
        try:
            logger.info(f"[KAFKA CONSUMER] Connecting to topic: {RESPONSE_TOPIC}")

            consumer = KafkaConsumer(
                RESPONSE_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                group_id="flask-websocket-consumer",
                auto_offset_reset="latest",
                enable_auto_commit=True
            )

            logger.info(f"[KAFKA CONSUMER] Listening on topic: {RESPONSE_TOPIC}")

            for record in consumer:
                try:
                    raw_message = record.value.decode("utf-8")
                    message = json.loads(raw_message)

                except Exception as error:
                    logger.error("=== INVALID KAFKA MESSAGE ===")
                    logger.error(f"[RAW MESSAGE] {record.value}")
                    logger.error(f"[ERROR] {str(error)}")
                    continue

                target_uuid = message.get("UUID")

                logger.info("=== KAFKA RESPONSE RECEIVED ===")
                logger.info(f"[TARGET UUID] {target_uuid}")
                logger.info(f"[PAYLOAD] {json.dumps(message)}")

                if not target_uuid:
                    logger.error("[WEBSOCKET] Response without uuid. Cannot route message.")
                    continue

                socketio.emit(
                    "kafka_response",
                    message,
                    room=target_uuid
                )

                logger.info("[WEBSOCKET] Response emitted successfully.")

        except Exception as error:
            logger.exception("=== KAFKA CONSUMER ERROR ===")
            logger.info("[KAFKA CONSUMER] Retrying connection in 5 seconds...")
            time.sleep(5)


# ==============================================================================
# MAIN
# ==============================================================================
if __name__ == "__main__":
    logger.info("=== STARTING FLASK BACKEND ===")

    kafka_thread = threading.Thread(target=listen_kafka_response_topic)
    kafka_thread.daemon = True
    kafka_thread.start()

    logger.info("[KAFKA CONSUMER] Background thread started.")
    logger.info("[FLASK] Starting server on 0.0.0.0:5001")

    socketio.run(
        app,
        host="0.0.0.0",
        port=5001,
        debug=True,
        use_reloader=False,
        allow_unsafe_werkzeug=True
    )