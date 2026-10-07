"""Phát lại dữ liệu real-time đã lưu vào Kafka, để C thử Spark Streaming trên máy mình.

  python replay.py export/raw          # đọc export/raw/<topic>/date=.../*.jsonl, gửi vào đúng topic
  python replay.py export/raw 0.05     # nghỉ 0,05 giây giữa hai message để giả lập luồng
"""
import json
import sys
import time
from pathlib import Path

from kafka import KafkaProducer

from common import KAFKA_BOOTSTRAP

folder = Path(sys.argv[1])
delay = float(sys.argv[2]) if len(sys.argv) > 2 else 0

producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
sent = 0
for path in sorted(folder.rglob("*.jsonl")):
    topic = path.relative_to(folder).parts[0]
    for line in path.read_text(encoding="utf-8").splitlines():
        msg = json.loads(line)
        key = str(msg.get("station_id") or msg.get("city")).encode()
        producer.send(topic, key=key, value=line.encode())
        sent += 1
        if delay:
            time.sleep(delay)
producer.flush()
print(f"Đã gửi {sent} message vào Kafka {KAFKA_BOOTSTRAP}")
