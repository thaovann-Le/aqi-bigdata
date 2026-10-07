"""Chép dữ liệu giữa MinIO và thư mục trên máy, dùng để gửi dữ liệu cho thành viên khác.

  python sync_minio.py export history            # MinIO bucket history -> export/history/
  python sync_minio.py export raw
  python sync_minio.py import export/history history   # thư mục -> bucket (người nhận chạy)

Cả hai chiều đều bỏ qua file đã có cùng kích thước, nên chạy lại chỉ chép phần mới.
"""
import sys
from pathlib import Path

from common import HERE, ensure_bucket, minio_client, scan

s3 = minio_client()


def export(bucket, root):
    copied = 0
    for obj in scan(s3, bucket):
        path = root / bucket / obj["Key"]
        if path.exists() and path.stat().st_size == obj["Size"]:
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, obj["Key"], str(path))
        copied += 1
    print(f"Đã chép {copied} file mới vào {root / bucket}")


def import_(folder, bucket):
    ensure_bucket(s3, bucket)
    existing = {o["Key"]: o["Size"] for o in scan(s3, bucket)}
    copied = 0
    for path in sorted(folder.rglob("*")):
        if not path.is_file() or path.name.startswith("."):
            continue
        key = path.relative_to(folder).as_posix()
        if existing.get(key) == path.stat().st_size:
            continue
        s3.upload_file(str(path), bucket, key)
        copied += 1
    print(f"Đã tải {copied} file mới lên bucket {bucket}")


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) >= 2 and args[0] == "export":
        export(args[1], Path(args[2]) if len(args) > 2 else HERE / "export")
    elif len(args) == 3 and args[0] == "import":
        import_(Path(args[1]), args[2])
    else:
        print(__doc__)
