

Latest This works & produces a collection==================

#generate date range to tranpose data sets

day_step = 60*60*24
#month_step = 31*60*60*24 #optional
ts_today = datetime.today() + timedelta(days=1)
ts_data = zip(
    ['2015-01-01'],
    [ts_today.strftime('%Y-%m-%d')]
)
df_dates= spark.createDataFrame(ts_data, ['start','end'])
df_ts=df_dates.select(F.col('start').cast('timestamp').alias('startdate'),F.col('end').cast('timestamp').alias('enddate'))
startdate = df_ts.first()[0]
enddate = df_ts.first()[1]
min_date, max_date = df_ts.select(F.col('startdate').cast("long"), F.col('enddate').cast("long")).first()
df_timestamps = spark.range(min_date,max_date,day_step)
df_timestamps = df_timestamps.select(F.to_date(F.col('id').cast('timestamp')).alias("date"))

#DEBUG
#df_timestamps.agg(F.max('date').alias('datemax')).show()
df_timestamps.createOrReplaceTempView("timestamps")
spark.sql("""
SELECT * FROM timestamps
""")





This works & produces a collection==================
month_step = 31*60*60*24
day_step = 60*60*24


# NOTE: need to add one day to include today
# NOTE: UTC time
ts_today = datetime.today() + timedelta(days=1)
ts_data = zip( \
              ['2015-01-01'], \
              [ts_today.strftime('%Y-%m-%d')] \
              )
df_dates= spark.createDataFrame(ts_data, ['start','end'])
df_ts=df_dates.select(F.col('start').cast('timestamp').alias('startdate'),F.col('end').cast('timestamp').alias('enddate'))
startdate = df_ts.first()[0]
enddate = df_ts.first()[1]
min_date, max_date = df_ts.select(F.col('startdate').cast("long"), F.col('enddate').cast("long")).first()
df_timestamps = spark.range(min_date,max_date,day_step)
df_timestamps = df_timestamps.select(F.to_date(F.col('id').cast('timestamp')).alias("date"))


#DEBUG
df_timestamps.agg(F.max('date').alias('datemax')).show()



This works==================

data = [ \
        ('1997-02-01 10:30:00','1997-02-04 10:30:00'), \
        ('1997-02-02 10:30:00','1997-02-05 10:30:00'), \
        ('1997-02-03 10:30:00','1997-02-06 10:30:00') \
        ]
rdd = spark.sparkContext.parallelize(data)
df= spark.createDataFrame(rdd, ['start','end'])
df.show()


This works too============
from pyspark import SparkContext
from pyspark.sql import functions as F
schema = StructType([ \
                     StructField("start",StringType(),True), \
                     ])
data =[('1997-06-22 21:00:00',)]

df = spark.createDataFrame(data, schema)
df.printSchema()
df1 = df.select(F.to_timestamp(df.start, 'yyyy-MM-dd HH:mm:ss').alias('dt'))
df1.printSchema()
df1.show()



This works too============
data2 = zip( \
            ['1997-02-01 10:30:00','1997-02-02 10:30:00','1997-02-03 10:30:00'], \
            ['1997-02-04 10:30:00','1997-02-05 10:30:00','1997-02-06 10:30:00','1997-02-06 09:30:00'] \
            )
df= spark.createDataFrame(data2, ['start','end'])
df.show()


This works too============

month_step = 31*60*60*24
day_step = 60*60*24
from pyspark.sql.functions import to_timestamp
data = zip( \
           ['1997-02-01 10:30:00'], \
           ['1997-02-04 10:30:00'] \
           )
df= spark.createDataFrame(data, ['start','end'])
df.show()
df2=df.select(df.start.cast('timestamp').alias('startdate'),df.end.cast('timestamp').alias('enddate'))
df2.printSchema()
startdate = df2.first()[0]
print(startdate)
enddate = df2.first()[1]
print(enddate)

min_date, max_date = df2.select(df2.startdate.cast("long"), df2.enddate.cast("long")).first()
df_ts = spark.range(min_date,max_date,day_step)
df_ts.select(df_ts.id.cast('timestamp').alias("dates")).show()

This works too============


from pyspark.sql import functions as F
df = spark.createDataFrame(data=zip( \
                                    ['1997-02-01 10:30:00'], \
                                    ['1997-02-01 10:31:00']), \
                           schema=['start','end'])
df1 = df.select(F.to_timestamp(df.start, 'yyyy-MM-dd HH:mm:ss').alias('start'))
df.select(F.to_timestamp(df.start, 'yyyy-MM-dd HH:mm:ss').alias('start')).collect()
df1.show()


This works too but single cell needs to be extracted============
%%sql
SELECT sequence(to_date('2018-01-01'), to_date('2018-03-01'), interval 1 day) as date
