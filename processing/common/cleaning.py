"""Hàm làm sạch dùng chung cho streaming, batch lịch sử và batch đọc lại dữ liệu thô.

Mọi hàm nhận/trả DataFrame, không có I/O, nên chạy được cả trong test cục bộ.
"""
from pyspark.sql import Column, DataFrame, Window
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


def hour_ending(col: Column) -> Column:
    """Mốc giờ T của khoảng (T-1h, T] chứa thời điểm `col`: số liệu của giờ T là số đo trong giờ KẾT THÚC lúc T.

    Đây là quy ước của cả hai nguồn: OpenAQ gắn trung bình giờ vào cuối giờ (trung bình giờ của OpenAQ cho trạm đo
    5 phút khớp trung bình các mẫu trong (T-1h, T], đối chiếu trên backfill.zip), Open-Meteo hourly cho giá trị tức
    thời lúc T và lượng mưa của giờ trước T. Số đo đúng đầu giờ giữ nguyên mốc (09:00 -> 09:00); mẫu lẻ phút thuộc
    giờ kế tiếp (08:05 -> 09:00).
    """
    t = F.date_trunc("hour", col)
    return F.when(col > t, t + F.expr("INTERVAL 1 HOUR")).otherwise(t)


def dedupe_latest(df: DataFrame, keys, order_col="fetched_at") -> DataFrame:
    """Với mỗi khóa giữ bản ghi có order_col lớn nhất (bản ghi trùng là chuyện bình thường, xem README của B)."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def merge_stored(new: DataFrame, stored: DataFrame, keys, scope, time_col, order_col="fetched_at") -> DataFrame:
    """Gộp dòng của lô hiện tại với dòng đã lưu từ các lô trước để tính lại TRỌN giờ.

    Chỉ lấy dòng đã lưu thuộc các (scope..., giờ) mà lô hiện tại chạm tới; cùng khóa thì giữ order_col lớn nhất
    (giống batch khử trùng trên toàn bộ dữ liệu), nên kết quả không phụ thuộc cách chia lô.
    """
    cols = new.columns
    touched = new.select(*scope, hour_ending(F.col(time_col)).alias("_h")).distinct()
    old = (stored.select(*cols).withColumn("_h", hour_ending(F.col(time_col)))
                 .join(touched, [*scope, "_h"], "left_semi").drop("_h"))
    return dedupe_latest(new.unionByName(old), keys, order_col)


def finalize_measurements(df: DataFrame) -> DataFrame:
    """Chuẩn hóa đầu ra về đúng cột bảng `measurements`.

    - Giá trị ngoài khoảng hợp lệ -> null.
    - Nguồn không có AQI sẵn (OpenAQ) -> tính US AQI từ PM2.5/PM10 theo giờ; nguồn có AQI mà để trống -> null.
    - Bỏ dòng không còn thông tin ô nhiễm nào (không pollutant, không AQI).
    """
    df = null_out_of_range(df, config.POLLUTANT_RANGES)
    df = null_out_of_range(df, config.WEATHER_RANGES)
    lo, hi = config.AQI_RANGE
    aqi = F.when(F.col("aqi").between(lo, hi), F.col("aqi"))
    computed = F.when(F.col("source").isin(*config.AQI_FROM_CONCENTRATION), us_aqi(F.col("pm25"), F.col("pm10")))
    df = df.withColumn("aqi", F.coalesce(aqi.cast("int"), computed))
    df = df.withColumn("aqi_standard", F.lit(config.AQI_STANDARD))
    any_pollution = F.greatest(*[F.col(c).isNotNull().cast("int") for c in config.POLLUTANTS + ("aqi",)]) > 0
    df = df.where(any_pollution)
    for c in config.WEATHER_COLS:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("double"))
    return df.select(*config.MEASUREMENT_COLS)


def attach_weather(aq: DataFrame, weather: DataFrame) -> DataFrame:
    """Ghép thời tiết vào số đo theo (city, ts). Left join: không có thời tiết thì để null."""
    w = weather.select("city", "ts", *config.WEATHER_COLS)
    base = aq.drop(*[c for c in config.WEATHER_COLS if c in aq.columns])
    return base.join(w, ["city", "ts"], "left")
