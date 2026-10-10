"""Hàm làm sạch dùng chung cho streaming, batch lịch sử và batch đọc lại dữ liệu thô.

Mọi hàm nhận/trả DataFrame, không có I/O, nên chạy được cả trong test cục bộ.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from common import config
from common.aqi import us_aqi


def in_range(col_name, lo, hi):
    """Giữ giá trị trong [lo, hi], còn lại thành null (gồm cả -999 = thiếu của OpenAQ)."""
    c = F.col(col_name)
    return F.when(c.between(lo, hi), c).otherwise(F.lit(None).cast("double"))


def null_out_of_range(df: DataFrame, ranges: dict) -> DataFrame:
    for name, (lo, hi) in ranges.items():
        if name in df.columns:
            df = df.withColumn(name, in_range(name, lo, hi))
    return df


def is_mass_conc(units_col):
    """Chỉ nhận đơn vị khối lượng/thể tích (µg/m³; Open-Meteo viết bằng chữ μ Hy Lạp, OpenAQ bằng µ micro)."""
    return F.lower(units_col).contains("g/m")


def dedupe_latest(df: DataFrame, keys, order_col="fetched_at") -> DataFrame:
    """Với mỗi khóa giữ bản ghi có order_col lớn nhất (bản ghi trùng là chuyện bình thường, xem README của B)."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def finalize_measurements(df: DataFrame) -> DataFrame:
    """Chuẩn hóa đầu ra về đúng cột bảng `measurements`.

    - Giá trị ngoài khoảng hợp lệ -> null.
    - Nguồn không có AQI sẵn (OpenAQ) -> tính US AQI từ PM2.5/PM10 theo giờ.
    - Bỏ dòng không còn thông tin ô nhiễm nào (không pollutant, không AQI).
    """
    df = null_out_of_range(df, config.POLLUTANT_RANGES)
    df = null_out_of_range(df, config.WEATHER_RANGES)
    lo, hi = config.AQI_RANGE
    aqi = F.when(F.col("aqi").between(lo, hi), F.col("aqi"))
    df = df.withColumn("aqi", F.coalesce(aqi.cast("int"), us_aqi(F.col("pm25"), F.col("pm10"))))
    df = df.withColumn("aqi_standard", F.lit(config.AQI_STANDARD))
    any_pollution = F.greatest(*[F.col(c).isNotNull().cast("int") for c in config.POLLUTANTS + ("aqi",)]) > 0
    df = df.where(any_pollution)
    for c in config.WEATHER_COLS:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("double"))
    return df.select(*config.MEASUREMENT_COLS)


def attach_weather(aq: DataFrame, weather: DataFrame) -> DataFrame:
    """Ghép thời tiết vào số đo theo (city, ts đầu giờ). Left join: không có thời tiết thì để null."""
    w = weather.select("city", "ts", *config.WEATHER_COLS)
    base = aq.drop(*[c for c in config.WEATHER_COLS if c in aq.columns])
    return base.join(w, ["city", "ts"], "left")
