import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]          # .../processing
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))
sys.path.insert(0, str(ROOT / "batch"))

from common import io  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.default.parallelism", "2")
         .config("spark.ui.enabled", "false")
         .config("spark.driver.memory", "1g")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def stations(spark):
    return io.load_stations(spark)


@pytest.fixture(scope="session")
def sample_history(tmp_path_factory):
    import make_sample_history
    out = tmp_path_factory.mktemp("history")
    make_sample_history.main(str(out))
    return str(out)


@pytest.fixture(scope="session")
def raw_sample(tmp_path_factory):
    """Dữ liệu real-time THẬT mà B đã đưa vào repo (ingestion/results/raw-mau.zip)."""
    z = ROOT.parent / "ingestion" / "results" / "raw-mau.zip"
    out = tmp_path_factory.mktemp("raw")
    with zipfile.ZipFile(z) as f:
        f.extractall(out)
    return out / "raw"


class Collector:
    """Sink giả: gom các dòng đã "ghi" theo tên bảng để test kiểm tra."""
    def __init__(self):
        self.tables = {}

    def __call__(self, df, table):
        self.tables.setdefault(table, []).extend(r.asDict() for r in df.collect())

    def rows(self, table):
        return self.tables.get(table, [])


@pytest.fixture()
def sink():
    return Collector()
