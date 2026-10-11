"""Real-time: đọc Kafka, gom lô, ghi JSON Lines vào MinIO bucket raw, chia thư mục theo ngày."""
import time
import uuid
from collections import defaultdict
from datetime import datetime
from zoneinfo import ZoneInfo

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from common import KAFKA_BOOTSTRAP, ensure_bucket, minio_client, setup_logging

log = setup_logging("consumer_minio")
BUCKET = "raw"
BATCH_SIZE = 500
FLUSH_SEC = 300
VN = ZoneInfo("Asia/Ho_Chi_Minh")

s3 = minio_client()
ensure_bucket(s3, BUCKET)

consumer = KafkaConsumer(
    "aqi_raw",
    "weather_raw",
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id="minio-sink",
    enable_auto_commit=False,
    auto_offset_reset="earliest",
)


def flush(buffers):
    now = datetime.now(VN)
    for topic, lines in buffers.items():
        key = f"{topic}/date={now:%Y-%m-%d}/{topic}-{now:%H%M%S}-{uuid.uuid4().hex[:8]}.jsonl"
        s3.put_object(Bucket=BUCKET, Key=key, Body="\n".join(lines).encode())
        log.info("Ghi %d bản ghi vào %s/%s", len(lines), BUCKET, key)


buffers = defaultdict(list)
last_flush = time.time()
log.info("Bắt đầu đọc Kafka %s", KAFKA_BOOTSTRAP)
while True:
    try:
        for tp, messages in consumer.poll(timeout_ms=1000).items():
            buffers[tp.topic].extend(m.value.decode() for m in messages)

        pending = sum(len(v) for v in buffers.values())
        if not pending:
            last_flush = time.time()
        elif pending >= BATCH_SIZE or time.time() - last_flush >= FLUSH_SEC:
            # Chỉ commit offset sau khi ghi MinIO thành công: chết giữa chừng thì đọc lại, không mất
            flush(buffers)
            buffers.clear()
            last_flush = time.time()
            consumer.commit()
    except KafkaError as e:
        # Hay gặp khi máy vừa ngủ dậy: Kafka đã loại consumer khỏi nhóm. Lần poll sau tự vào lại nhóm;
        # lô chưa commit sẽ được đọc lại nên có thể trùng, không mất.
        log.warning("Lỗi Kafka, tiếp tục sau 10 giây: %r", e)
        time.sleep(10)
    except Exception:
        log.exception("Lỗi khi ghi dữ liệu, thử lại sau 30 giây")
        time.sleep(30)
