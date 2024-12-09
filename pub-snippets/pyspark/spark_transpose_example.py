%%pyspark
#pipeline parameters
processdate  = '1999-12-16'
#readall=0 # read all partitions

%%pyspark
#imports
from datetime import date, datetime, timedelta, time
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.ml.feature import Bucketizer

%%pyspark
#DEBUG
print('today: ' + datetime.today().strftime('%Y-%m-%d'))
print('processdate: ' + processdate)


%%pyspark
#validate parameters
#ensure UTC
spark.conf.set('spark.sql.session.timeZone', 'UTC')
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.sql.parquet.compression.codec.", "snappy")
#trim time from date does
#note: built for daily batch
startTime = startTime if ' ' not in startTime else startTime.split(' ')[0]
endTime = endTime if ' ' not in endTime else endTime.split(' ')[0]
#ensure end date is not in the future
tomorrow = datetime.combine(date.today(), time.min) + timedelta(days=1)
dateObjStartTime = datetime.strptime(startTime,'%Y-%m-%d')
dateObjEndTime = datetime.strptime(endTime,'%Y-%m-%d')
startTime = startTime if dateObjStartTime < tomorrow else datetime.today().strftime('%Y-%m-%d')
endTime = endTime if dateObjEndTime < tomorrow else datetime.today().strftime('%Y-%m-%d')
#push out end date if dates are the same
dateObjEndTime = datetime.strptime(endTime,'%Y-%m-%d')
endTime = endTime if endTime > startTime else datetime.combine(dateObjEndTime,time.min).strftime('%Y-%m-%d')
#DEBUG
#print(dateObjStartTime)
#print(dateObjEndTime)

%%pyspark
#generate date range to tranpose data sets
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
df_timespan = df_timespan.select(F.to_date(F.col('id').cast('timestamp')).alias("rangedate"))

#DEBUG
#df_timespan.agg(F.max('date').alias('datemax')).show()
#df_timespan.createOrReplaceTempView("timespan")
#spark.sql("""
#        SELECT * FROM timespan
#    """)
#df_timespan.show(50)

%%pyspark
#readall or only required partitions based on parameter
#DEBUG
#startTime = '2020-12-02'
#endTime  = '2021-12-02'
#readall=-1
#dateObjStartTimeWindow = datetime.strptime(startTime,'%Y-%m-%d')
dateObjStartTimeWindow = datetime.combine(datetime.strptime(startTime,'%Y-%m-%d') + timedelta(days=-91), datetime.min.time())
dateObjEndTimeWindow = datetime.strptime(endTime,'%Y-%m-%d')


#read all data or else read (startTime - 90 days) to endTime
if readall == 1:
    df_useractivity = spark.read.load(
        '/bronze/db/tblUserActivity/*',
        #inferSchema='true',
        format='parquet',
        basePath = '/bronze/db/tblUserActivity/'
    )
else :
    df_useractivity = spark.read.load(
            '/bronze/db/tblUserActivity*',
            #inferSchema='true',
            format='parquet',
            basePath = '/tblUserActivity/'
        ).filter(
            (F.col('zPartKey') >= dateObjStartTimeWindow) & #NOTE: start day is moved back 90 days to allow the query to locate "resurrected users" from the required partitions
            (F.col('zPartKey') < dateObjEndTimeWindow)
        )
#DEBUG
#df_useractivity.show(20)

%%pyspark
# 86400 seconds in a day
days = lambda i: i * 86400
# a = 10 previous occursnces of user and b = 1 for first date
resurrectCheck = F.udf(lambda a,b: 0 if a == 1 or b > 0 else 1)
isFirstDate = F.udf(lambda a,b: 1 if a==b else 0)
checkAnonymous = F.udf(lambda a,b: a if a != None else b)
isAnonymous = F.udf(lambda a: 1 if a == None else 0)
addDays = F.udf(lambda a,b: datetime.combine(a + timedelta(days=b), datetime.min.time()),TimestampType())


%%pyspark
#summarise data
df_dailyuserdata = df_useractivity \
    .withColumn(
        'date', F.to_date(F.col('activity_datetime'))
    ).select(
        F.col('date'),
        isAnonymous(F.col('user_id').cast(StringType())).alias('anonymous'),
        checkAnonymous(F.col('user_id').cast(StringType()),F.col('device_ip')).alias('user')
    ).groupBy(
        'date','user'
    ).agg(
        F.max('anonymous').alias("anonymous")
    )
df_dailyuserdata.filter(
        (F.to_timestamp(F.col('date')) >= dateObjStartTime) &
        (F.to_timestamp(F.col('date')) < dateObjEndTime)
    ).write.save(
        mergeSchema='true',
        format='parquet',
        compression='snappy',
        mode='overwrite',
        path='/surface/db/tblDistinctUser/',
        partitionBy='date'
    )
#DEBUG


%%pyspark
print(dateObjStartTime)
print(dateObjEndTime)



%%pyspark
df_dailydistinctuserevent = spark.read.load(
    '/surface/db/tblDistinctUser/*',
    inferSchema='true',
    format='parquet',
    basePath = '/surface/db/tblDistinctUser/'
)
#TODO create a cache of first date for users to prevent scanning full range using window/rangebetween
df_dailysummary = df_dailydistinctuserevent \
    .withColumn(
        'unix_time',
        F.col('date').cast('timestamp').cast('long')
    ).withColumn(
        'firstdate',
        F.first('date').over(Window.partitionBy('user').orderBy(F.col('unix_time')).rangeBetween(-days(1825),-days(0))) # check previous 90 days
    ).withColumn(
        'isfirstdate_val',
        isFirstDate(F.col('firstdate'),F.col('date'))
    ).withColumn(
        'isfirstdate_bool',
        F.col('isfirstdate_val').cast('Boolean')
    ).withColumn(
        'isresurrected_val',
        resurrectCheck(F.col('isfirstdate_val').cast('int'), F.count('user').over(Window.partitionBy('user').orderBy(F.col('unix_time')).rangeBetween(-days(91),-days(1)))) # check previous 90 days
    ).withColumn(
        'isresurrected_bool',
        F.col('isresurrected_val').cast('Boolean') # check previous 90 days
    ).select(
        '*'
    )
df_dailysummary.show(100)

#df_output= df_timespan.join(df_dailysummary, df_timespan.rangedate == df_dailysummary.date,how="left")
#df_output.filter(F.col('user').isNotNull()).sort(F.col('user')).show(50)
#bucketizer = Bucketizer(splits=[ 0, 6, 18, 60, float('Inf') ],inputCol="ages", outputCol="buckets")
#df_buck = bucketizer.setHandleInvalid("keep").transform(df)
#t = {0.0:"infant", 1.0: "minor", 2.0:"adult", 3.0: "senior"}
#udf_foo = udf(lambda x: t[x], StringType())
#df_buck.withColumn("age_bucket", udf_foo("buckets"))



%%pyspark
df_dailydistinctuserevent = spark.read.load(
    '/surface/db/tblDistinctUser/*',
    inferSchema='true',
    format='parquet',
    basePath = '/surface/db/tblDistinctUser/'
)
#TODO create a cache of first date for users to prevent scanning full range using window/rangebetween
df = df_dailydistinctuserevent \
    .withColumn(
        'unix_time',
        F.col('date').cast('timestamp').cast('long')
    ).withColumn(
        'firstdate',
        F.first('date').over(Window.partitionBy('user').orderBy(F.col('unix_time')).rangeBetween(-days(1825),-days(0))) # check previous 90 days
    ).select(
        isFirstDate(F.col('firstdate'),F.col('date')).alias('isfirstdate_val'),
        #Partition Style UDF
        resurrectCheck(isFirstDate(F.col('firstdate'),F.col('date')).cast('int'), F.count('user').over(Window.partitionBy('user').orderBy(F.col('unix_time')).rangeBetween(-days(91),-days(1)))).alias('isresurrected_val')
    )
df_dailysummary.show(100)

df_dailydistinctuserevent = spark.read.load(
    '/surface/db/tblDistinctUser/*',
    inferSchema='true',
    format='parquet',
    basePath = '/surface/db/tblDistinctUser/'
)
df_dailydistinctuserevent.filter(
    (F.to_timestamp(F.col('date')) >= dateObjStartTime) &
    (F.to_timestamp(F.col('date')) <= dateObjEndTime)
).count()


#final group by to get a daily summary, fill nulls and format for output
df_output = df_dailysummary \
    .groupBy(
        'date'
    ).agg(
        F.count('user').cast('int').alias("Total Active Users"),
        F.sum('isresurrected_val').cast('int').alias("Resurrected Users"),
        F.sum('isfirstdate_val').cast('int').alias("New Users")
    ).withColumn(
        'Existing Users',
        F.col('Total Active Users') - F.col('Resurrected Users') - F.col('New Users')
    ).orderBy(F.col('date')).select(
        F.col('date'),
        F.year(F.col('date')).alias('Year'),
        F.month(F.col('date')).alias('Month'),
        F.dayofmonth(F.col('date')).alias('Day'),
        F.form(F.col('date'),'y-M').alias('Year-Month'),
        F.date_format(F.col('date'),'Q').alias('Quarter'),
        F.date_format(F.col('date'),'E').alias("Day of Week")
        F.col('Total Active Users'),
            F.col('New Users'),
            F.col('Existing Users'),
            F.col('Resurrected Users')
        )
        df_output= df_timespan.join(df_output, df_timespan.rangedate == df_output.date,how="left")
        #fill null values with 0
        df_output = df_output.na.fill(0)
        #DEBUG
        #df_output.show()

        %%pyspark
        df_output.filter(
                (F.to_timestamp(F.col('date')) >= dateObjStartTime) &
                (F.to_timestamp(F.col('date')) < dateObjEndTime)
            ).write.save(
                mergeSchema='true',
                format='parquet',
                compression='snappy',
                mode='overwrite',
                path='/surface/db/tblSummary',
                partitionBy='date'
            )


















































