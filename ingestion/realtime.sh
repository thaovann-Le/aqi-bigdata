#!/bin/bash
# Bật lại (nếu cần) và kiểm tra phần thu thập real-time.
# Chạy sau mỗi lần mở máy, và mỗi sáng, mỗi tối:  ./realtime.sh
cd "$(dirname "$0")" || exit 1
COMPOSE="docker compose -f docker-compose.dev.yml"

if ! docker info > /dev/null 2>&1; then
  echo "Docker chưa chạy. Đang mở Docker Desktop; chờ 1-2 phút rồi chạy lại ./realtime.sh"
  open -a Docker
  exit 1
fi

$COMPOSE up -d
echo "Chờ Kafka sẵn sàng..."
for _ in $(seq 1 30); do
  if $COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo
for script in producer.py consumer_minio.py; do
  if pgrep -f "[Pp]ython $script" > /dev/null; then
    echo "$script: đang chạy"
  else
    echo "$script: KHÔNG chạy -> bật lại"
    caffeinate -i nohup .venv/bin/python "$script" > /dev/null 2>&1 &
  fi
done

echo
echo "Bây giờ:            $(date '+%Y-%m-%d %H:%M:%S')"
echo "Lần gửi gần nhất:   $(grep 'Đã gửi' producer.log | tail -n 1 | sed -E 's/,[0-9]+ INFO producer:/ /')"
echo "Lần ghi gần nhất:   $(grep 'Ghi' consumer_minio.log | tail -n 1 | cut -c1-19)"
echo
echo "Số chu kỳ theo ngày (một ngày chạy đủ là khoảng 144):"
grep 'Đã gửi' producer.log | cut -c1-10 | sort | uniq -c
