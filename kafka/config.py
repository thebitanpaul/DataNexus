# config.py — local Kafka connection settings.
# Local broker, no authentication needed. Nothing secret here, so this file
# is safe to commit to GitHub.

CONFIG = {
    "bootstrap.servers": "localhost:9092",
}

TOPIC = "new_orders"
