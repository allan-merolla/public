%%pyspark
#imports
from datetime import date, datetime, timedelta, time
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.ml.feature import Bucketizer
from delta.tables import *



%%pyspark
#validate parameters
#ensure UTC
spark.conf.set('spark.sql.session.timeZone', 'UTC')
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.shuffle.partitions", "600")
#spark.conf.set("spark.sql.parquet.writeLegacyFormat","true")
#https://spark.apache.org/docs/latest/configuration.html
#trim time from date does



#DEBUG
#print(dateObjStartTime)
#print(dateObjEndTime)

%%pyspark
#pipeline parameters
startTime = '1999-12-01'
endTime  = '2200-01-20'
#note: built for daily batch
startTime = startTime if ' ' not in startTime else startTime.split(' ')[0]
endTime = endTime if ' ' not in endTime else endTime.split(' ')[0]

#ensure end date is not in the future
tomorrow = datetime.combine(date.today(), time.min) + timedelta(days=1)
dateObjStartTime = datetime.strptime(startTime,'%Y-%m-%d')
dateObjEndTime = datetime.strptime(endTime,'%Y-%m-%d')
startTime = startTime if dateObjStartTime < tomorrow else datetime.today().strftime('%Y-%m-%d')
endTime = endTime if dateObjEndTime < tomorrow else datetime.today().strftime('%Y-%m-%d')
day_step = 60*60*24
#month_step = 31*60*60*24 #optional
#endtime is endtime+1day to ensure daterange as spark.range will generate the date before last day
ts_today = datetime.today() + timedelta(days=1)
ts_data = zip(
    [startTime],
    [endTime]
)
df_dates= spark.createDataFrame(ts_data, ['start','end'])
df_ts=df_dates.select(F.col('start').cast('timestamp').alias('startdate'),F.col('end').cast('timestamp').alias('enddate'))
startdate = df_ts.first()[0]
enddate = df_ts.first()[1]
min_date, max_date = df_ts.select(F.col('startdate').cast("long"), F.col('enddate').cast("long")).first()
df_timespan = spark.range(min_date,max_date,day_step)
df_timespan = df_timespan.select(
    F.to_date(F.col('id').cast('timestamp')).alias("rangedate"),
    F.date_format(F.col('id').cast('timestamp'),'yyyy-MM').alias("year-month"))
date_col = df_timespan.select('rangedate').distinct().rdd.map(lambda row: row[0]).collect()
yearmonth_col=df_timespan.select('year-month').distinct().rdd.map(lambda row : row[0]).collect()
#print(date_col)
#print(yearmonth_col)
for date in date_col:
    spark.read.load(
            'abfss://data@data.dfs.core.windows.net/raw/activity_datetime=' +str(date) +'',
            inferSchema='true',
            format='parquet'
        ).withColumn(
            'activity_datetime',F.expr('to_timestamp(from_unixtime(int(injested/1000)))').cast(TimestampType())
        ).select(
            F.col('uuid').alias('tracking_id').cast(StringType()),
            F.col('activity_datetime').cast(DateType()).alias('activity_date'),
            'activity_datetime',
            F.col('type').alias('type').cast(StringType()),

            F.expr("get_json_object(context, '$.account.id')").alias('account_id').cast(StringType()),
            F.expr("get_json_object(context, '$.anonymous.id')").alias('anonymous_id').cast(StringType()),
            F.expr("get_json_object(context, '$.user.id')").alias('user_id').cast(StringType()),
            F.expr("get_json_object(context, '$.session.id')").alias('session_id').cast(StringType()),
            F.expr("get_json_object(context, '$.search.id')").alias('search_id').cast(StringType()),
            F.expr("get_json_object(context, '$.timetracking.id')").alias('timetracking_id').cast(StringType()),
            F.expr("get_json_object(context, '$.request.clientIP')").alias('request_clientIP').cast(StringType()),
            F.expr("get_json_object(context, '$.request.userAgent')").alias('request_userAgent').cast(StringType()),
            F.expr("get_json_object(context, '$.application.repo')").alias('application_repo').cast(StringType()),
            F.expr("get_json_object(context, '$.application.name')").alias('application_name').cast(StringType()),
            F.expr("get_json_object(context, '$.application.page')").alias('application_page').cast(StringType()),
            F.expr("get_json_object(context, '$.application.visibility')").alias('application_visibility').cast(StringType()),
            F.expr("get_json_object(context, '$.page.title')").alias('page_title').cast(StringType()),
            F.expr("get_json_object(context, '$.page.url')").alias('page_url').cast(StringType()),
            F.expr("get_json_object(context, '$.page.referrer')").alias('page_referrer').cast(StringType()),
            F.expr("get_json_object(context, '$.element_tid')").alias('page_element').cast(StringType()),
            F.expr("get_json_object(context, '$.timetracking.created')").alias('timetracking_created').cast(StringType()),
            F.expr("get_json_object(context, '$.timetracking.name')").alias('timetracking_name').cast(StringType()),
            F.expr("get_json_object(context, '$.timetracking.timeSinceStart')").alias('timetracking_timeSinceStart').cast(StringType()),

            F.col('context').alias('context').cast(StringType()),
            F.lit('1999-01-17 00:00:00').alias('zLoadDate').cast(StringType()),    #TODO update me with each run
            F.lit('PLIN0004').alias('zPipeline').cast(StringType()),
            F.col('activity_datetime').alias('zPartKey').cast(DateType()),
        ).repartition('zPartKey').write.saveAsTable(
            'db.bronze_tblActivity',
            mergeSchema='true',
            format='parquet',
            compression='snappy',
            mode='append',
            path='/source/bronze/tblActivity', #differs with _ to align with container names which cannot have "_"
            partitionBy='zPartKey'
        )
