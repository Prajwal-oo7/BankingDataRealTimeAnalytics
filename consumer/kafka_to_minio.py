import json
import logging
import os
import time, uuid
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException, KafkaError, TopicPartition
from minio import Minio
from minio.error import S3Error
import pyarrow as pa
import pyarrow.parquet as pq

# ==========================================================
# Load Environment Variables
# ==========================================================

load_dotenv()

# ==========================================================
# Logging
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ==========================================================
# Configuration
# ==========================================================

TOPICS = [
    "banking.public.customers",
    "banking.public.accounts",
    "banking.public.transactions"
]

FLUSH_SIZE = 50
FLUSH_INTERVAL = 60          # seconds
MAX_BUFFER_SIZE=5000

LOCAL_STAGING = Path("./tmp")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "banking-data")


# ==========================================================
# Helper Functions
# ==========================================================

def current_timestamp():
    return datetime.now(timezone.utc).isoformat()

def table_name(topic):
    return topic.split(".")[-1]

def parquet_filename(topic):
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    unique = uuid.uuid4().hex[:8]
    return f"{table_name(topic)}_{ts}_{unique}.parquet"


# ==========================================================
# Kafka Consumer
# ==========================================================

consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    "group.id": os.getenv("KAFKA_GROUP_ID", "banking-minio-consumer"),
    "client.id": "banking-minio-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})
consumer.subscribe(TOPICS)
logger.info("Kafka Consumer Connected.")

# ==========================================================
# MinIO Client
# ==========================================================

minio_client = Minio(
    endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
    secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    secure=False
)
logger.info("Connecting to MinIO...")

# Ensure MinIO is ready with retries
for attempt in range(1, 11):
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET):
            minio_client.make_bucket(MINIO_BUCKET)
            logger.info(f"Created bucket : {MINIO_BUCKET}")
        else:
            logger.info(f"Using existing bucket : {MINIO_BUCKET}")
        break
    except Exception as e:
        logger.warning(f"MinIO not ready ({attempt}/10). Retrying in 3s...")
        time.sleep(3)

else:
    raise RuntimeError("Unable to connect to MinIO after 10 attempts.")


# ==========================================================
# Local Staging Directories
# ==========================================================

for topic in TOPICS:
    table = topic.split(".")[-1]
    (LOCAL_STAGING / table).mkdir(
        parents=True,
        exist_ok=True
    )
# ==========================================================
# Topic Buffers
# ==========================================================

# 2. State Management (FIXED: Multi-Partition Offset Tracking)
buffers = {topic: [] for topic in TOPICS}
last_offsets = {topic: {} for topic in TOPICS}  # Dict of {partition_id: KafkaMessage}
last_flush_time = {topic: time.time() for topic in TOPICS}

# ==========================================================
# Convert Buffer -> PyArrow Table
# ==========================================================

def build_arrow_table(records):
    return pa.Table.from_pydict({
        "year": [r["year"] for r in records],
        "month": [r["month"] for r in records],
        "day": [r["day"] for r in records],
        "hour": [r["hour"] for r in records],

        "ingestion_timestamp": [r["ingestion_timestamp"] for r in records],

        "kafka_topic": [r["kafka_topic"] for r in records],

        "kafka_partition": [r["kafka_partition"] for r in records],

        "kafka_offset": [r["kafka_offset"] for r in records],

        "kafka_key": [r["kafka_key"] for r in records],

        "kafka_timestamp": [r["kafka_timestamp"] for r in records],

        "payload": [r["payload"] for r in records]
    })

# ==========================================================
# Buffer Message : Core Processing Logic
# ==========================================================

def buffer_message(msg):
    topic = msg.topic()
    partition = msg.partition()

    # Handle Kafka Timestamps
    timestamp_type, kafka_ts = msg.timestamp()              #(timestamp_type, timestamp_ms)
    if kafka_ts: kafka_ts = datetime.fromtimestamp(kafka_ts / 1000,tz=timezone.utc).isoformat()
    else: kafka_ts = None
    
    # FIXED: Handle Debezium Delete Tombstones (value is None)
    raw_value = msg.value()
    payload_str = raw_value.decode("utf-8", errors="replace") if raw_value else None

    record = {
        "ingestion_timestamp": current_timestamp(),
        "kafka_topic": topic,
        "kafka_partition": msg.partition(),
        "kafka_offset": msg.offset(),
        "kafka_key": msg.key().decode("utf-8", errors="replace") if msg.key() else None,
        "kafka_timestamp": kafka_ts,
        "payload": payload_str
    }

    buffers[topic].append(record)

    # FIXED: Track highest offset per partition, not just per topic
    last_offsets[topic][partition] = msg

    if len(buffers[topic]) >= MAX_BUFFER_SIZE:
        logger.warning(f"{table_name(topic)} buffer reached {MAX_BUFFER_SIZE} limits. Forcing flush.")
        flush_topic(topic)


# ==========================================================
# Flush One Topic
# ==========================================================

def flush_topic(topic):

    records = buffers[topic]

    if not records:
        return

    logger.info(f"Flushing {len(records)} records from {table_name(topic)}")

    # FIXED: Generate UUID and timestamp ONCE per flush to ensure Local/MinIO paths match perfectly
    now = datetime.now(timezone.utc)
    hive_part = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
    filename=parquet_filename(topic)

    local_path = LOCAL_STAGING / table_name(topic) / filename
    object_name = f"raw/{table_name(topic)}/{hive_part}/{filename}"

    # Step A: Write Local Parquet
    try:
        for record in records:
            record["year"] = now.year
            record["month"] = now.month
            record["day"] = now.day
            record["hour"] = now.hour
        
        table = build_arrow_table(records)
        pq.write_table(table, str(local_path), compression="snappy")

    except Exception as e:
        logger.error(f"Failed writing local parquet for {topic}: {e}")
        return # Skip upload and keep buffer

    # Step B: Upload to MinIO
    uploaded = False
    for attempt in range(1, 6):
        try:
            minio_client.fput_object(
                bucket_name=MINIO_BUCKET,
                object_name=object_name,
                file_path=str(local_path)
            )
            uploaded = True
            logger.info(f"Uploaded {len(records)} records to {object_name}")
            break

        except S3Error as e:
            logger.warning(f"Upload attempt {attempt}/5 failed : {e}")
            time.sleep(3)

    if not uploaded:
        logger.error(
            "Upload failed. Buffer retained. "
            "Kafka offsets NOT committed."
        )
        return

    # Step C: Commit Kafka Offsets (Multi-Partition Aware)
    try:
        # Commit the next offset to consume (+1) for every partition we processed in this batch
        partitions_to_commit = [
            TopicPartition(topic, part, msg.offset() + 1)
            for part, msg in last_offsets[topic].items()
        ]
        
        if partitions_to_commit:
            consumer.commit(offsets=partitions_to_commit, asynchronous=False)
            logger.info(f"Committed {len(partitions_to_commit)} partitions for {table_name(topic)}")

    except KafkaException as e:
        logger.error(f"Offset commit failed: {e}. Data is in MinIO, but Kafka might resend it on restart.")
        # We don't return here; we still want to clear the buffer so we don't upload the same data twice while running

    # Step D: Cleanup & Reset State
    finally:
        if local_path.exists():
            local_path.unlink()
            
    buffers[topic].clear()
    last_offsets[topic].clear()
    last_flush_time[topic] = time.time()


# ==========================================================
# Check Flush Conditions
# ==========================================================

def check_flush():
    current = time.time()
    for topic in TOPICS:
        if len(buffers[topic]) >= FLUSH_SIZE:
            flush_topic(topic)
        elif buffers[topic] and (current - last_flush_time[topic] >= FLUSH_INTERVAL):
            flush_topic(topic)


# ==========================================================
# Main Consumer Loop
# ==========================================================

def consume():
    logger.info("Starting Kafka -> MinIO ingestion...")

    while True:
        msg = consumer.poll(0.5)
        check_flush()

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error(f"Kafka error: {msg.error()}")
            continue

        try:
            buffer_message(msg)

        except Exception as e:
            logger.error(f"Message processing failed at offset {msg.offset()}: {e}", exc_info=True)


# ==========================================================
# Shutdown
# ==========================================================

def shutdown():
    logger.info("Gracefully shutting down...")

    for topic in TOPICS:
        if buffers[topic]:
            logger.info(f"Flushing final buffer for {topic}")
            flush_topic(topic)

    consumer.close()
    logger.info("Consumer closed.")


# ==========================================================
# Main
# ==========================================================

if __name__ == "__main__":
    try: consume()
    except KeyboardInterrupt: logger.info("Shutdown requested via KeyboardInterrupt.")
    finally: shutdown()