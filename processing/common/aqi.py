"""Tính US AQI từ nồng độ PM2.5/PM10 và phân mức cảnh báo.

Công thức tuyến tính theo từng khoảng:  AQI = (Ihi-Ilo)/(Chi-Clo) * (C-Clo) + Ilo.
Bảng điểm gãy được dùng chung giữa bản Spark (biểu thức cột, không dùng UDF) và bản Python
thuần (`aqi_py`, dùng để đối chiếu trong test), nên hai bản không thể lệch nhau.

Giới hạn cần nêu trong báo cáo: AQI ở đây tính từ nồng độ THEO GIỜ. AQICN và Open-Meteo công bố
AQI theo trung bình 24 giờ/NowCast nên số của họ mượt hơn và không bằng số tính ở đây.
Chỉ dùng PM2.5 và PM10 (CO/NO2/SO2/O3 cần đổi sang ppm/ppb và thời gian lấy trung bình riêng).
"""
import math

from pyspark.sql import Column
from pyspark.sql import functions as F

from common import config

# (C_lo, C_hi, I_lo, I_hi)
PM25_2024 = [(0.0, 9.0, 0, 50), (9.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
             (55.5, 125.4, 151, 200), (125.5, 225.4, 201, 300), (225.5, 325.4, 301, 500)]
PM25_2012 = [(0.0, 12.0, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
             (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300), (250.5, 350.4, 301, 400),
             (350.5, 500.4, 401, 500)]
PM10 = [(0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150),
        (255, 354, 151, 200), (355, 424, 201, 300), (425, 604, 301, 500)]


def _pm25_table():
    return PM25_2012 if config.PM25_BREAKPOINTS == "2012" else PM25_2024


def _py_sub_index(c, table, truncate):
    if c is None or c < 0:
        return None
    c = truncate(c)
    for c_lo, c_hi, i_lo, i_hi in table:
        if c <= c_hi:
            # làm tròn half-up giống F.round của Spark
            return math.floor((i_hi - i_lo) / (c_hi - c_lo) * (c - c_lo) + i_lo + 0.5)
    return table[-1][3]                 # vượt khoảng cuối: chặn ở mức tối đa


def aqi_py(pm25=None, pm10=None):
    """Bản Python thuần: AQI = max(chỉ số con PM2.5, chỉ số con PM10); None nếu cả hai thiếu."""
    subs = [s for s in (_py_sub_index(pm25, _pm25_table(), lambda c: math.floor(c * 10) / 10),
                        _py_sub_index(pm10, PM10, math.floor)) if s is not None]
    return max(subs) if subs else None


def _sub_index(col: Column, table, truncate) -> Column:
    c = truncate(col)
    expr = F.lit(None).cast("double")
    # Duyệt từ khoảng thấp lên cao: khoảng cao nhất được kiểm tra trước (nằm ngoài cùng).
    for c_lo, c_hi, i_lo, i_hi in table:
        slope = float(i_hi - i_lo) / float(c_hi - c_lo)
        expr = F.when(c >= F.lit(float(c_lo)), F.lit(slope) * (c - F.lit(float(c_lo))) + F.lit(float(i_lo))).otherwise(expr)
    top = F.lit(float(table[-1][3]))
    return F.when(col.isNull() | (col < 0), F.lit(None).cast("double")).otherwise(F.least(F.round(expr), top))


def us_aqi(pm25: Column, pm10: Column) -> Column:
    """Cột Spark (int): US AQI từ PM2.5 và PM10 theo giờ. Null nếu cả hai đều thiếu."""
    s25 = _sub_index(pm25, _pm25_table(), lambda c: F.floor(c * 10) / 10)   # EPA cắt PM2.5 về 1 chữ số thập phân
    s10 = _sub_index(pm10, PM10, lambda c: F.floor(c))                      # và PM10 về số nguyên
    # greatest() bỏ qua null, chỉ trả null khi cả hai null.
    return F.greatest(s25, s10).cast("int")


def alert_level(aqi: Column) -> Column:
    return (F.when(aqi > 300, "hazardous")
             .when(aqi > 200, "very_unhealthy")
             .when(aqi > 150, "unhealthy")
             .when(aqi > 100, "unhealthy_for_sensitive")
             .when(aqi > 50, "moderate")
             .otherwise("good"))
