##If you use Python the shortest way to get things done is to re-use existing Pandas functions, with GROUPED_MAP udf:

from operator import attrgetter
from pyspark.sql.types import StructType
from pyspark.sql.functions import pandas_udf, PandasUDFType
def resample(schema, freq, timestamp_col = "timestamp",**kwargs):
    @pandas_udf(
        StructType(sorted(schema, key=attrgetter("name"))),
        PandasUDFType.GROUPED_MAP)
    def _(pdf):
        pdf.set_index(timestamp_col, inplace=True)
        pdf = pdf.resample(freq).interpolate()
        pdf.ffill(inplace=True)
        pdf.reset_index(drop=False, inplace=True)
        pdf.sort_index(axis=1, inplace=True)
        return pdf
    return _
from pyspark.sql.functions import to_timestamp
df = spark.createDataFrame([
    ("John",   "2018-02-01 03:00:00", 60),
    ("John",   "2018-02-01 03:03:00", 66),
    ("John",   "2018-02-01 03:05:00", 70),
    ("John",   "2018-02-01 03:08:00", 76),
    ("Mo",     "2017-06-04 01:05:00", 10),
    ("Mo",     "2017-06-04 01:07:00", 20),
    ("Mo",     "2017-06-04 01:10:00", 35),
    ("Mo",     "2017-06-04 01:11:00", 40),
], ("webID", "timestamp", "counts")).withColumn(
        "timestamp", to_timestamp("timestamp")
    )
df.groupBy("webID").apply(resample(df.schema, "60S")).show()
+------+-------------------+-----+
|counts|          timestamp|webID|
+------+-------------------+-----+
|    60|2018-02-01 03:00:00| John|
|    62|2018-02-01 03:01:00| John|
|    64|2018-02-01 03:02:00| John|
|    66|2018-02-01 03:03:00| John|
|    68|2018-02-01 03:04:00| John|
|    70|2018-02-01 03:05:00| John|
|    72|2018-02-01 03:06:00| John|
|    74|2018-02-01 03:07:00| John|
|    76|2018-02-01 03:08:00| John|
|    10|2017-06-04 01:05:00|   Mo|
|    15|2017-06-04 01:06:00|   Mo|
|    20|2017-06-04 01:07:00|   Mo|
|    25|2017-06-04 01:08:00|   Mo|
|    30|2017-06-04 01:09:00|   Mo|
|    35|2017-06-04 01:10:00|   Mo|
|    40|2017-06-04 01:11:00|   Mo|
+------+-------------------+-----+
partial = (df
           .groupBy("webID", window("timestamp", "5 minutes", "3 minutes")["start"])
           .apply(resample(df.schema, "60S")))
and aggregating the final result
from pyspark.sql.functions import mean
(partial
 .groupBy("webID", "timestamp")
 .agg(mean("counts")
      .alias("counts"))
 # Order by key and timestamp, only for consistent presentation
 .orderBy("webId", "timestamp")
 .show())
+-----+-------------------+------+
|webID|          timestamp|counts|
+-----+-------------------+------+
| John|2018-02-01 03:00:00|  60.0|
| John|2018-02-01 03:01:00|  62.0|
| John|2018-02-01 03:02:00|  64.0|
| John|2018-02-01 03:03:00|  66.0|
| John|2018-02-01 03:04:00|  68.0|
| John|2018-02-01 03:05:00|  70.0|
| John|2018-02-01 03:08:00|  76.0|
|   Mo|2017-06-04 01:05:00|  10.0|
|   Mo|2017-06-04 01:06:00|  15.0|
|   Mo|2017-06-04 01:07:00|  20.0|
|   Mo|2017-06-04 01:08:00|  25.0|
|   Mo|2017-06-04 01:09:00|  30.0|
|   Mo|2017-06-04 01:10:00|  35.0|
|   Mo|2017-06-04 01:11:00|  40.0|
+-----+-------------------+------+
