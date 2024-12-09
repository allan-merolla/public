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
spark.conf.set('spark.sql.session.timeZone', 'UTC')
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.shuffle.partitions", "600")

month="01"
dayStart=15
dayEnd=32
hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()

for i in range(dayStart,dayEnd):
    day = ""
    if len(str(i)) < 2:
        day = "0" + str(i)
    else:
        day = str(i)
    inputDir = '/db/data/zPartKey=2021-'+month+'-'+day
    outputDir = '/db/data/zPartKey=2021-'+month+'-'+day
    print(inputDir)
    print(outputDir)
    try:
        # path = hadoop.fs.Path(inputDir)
        #fs.get(conf).listStatus(path)
        #for f in fs.get(conf).listStatus(path):
        #    print('File to process:' +str(f.getPath()))
        df = spark.read.parquet(inputDir) #str(f.getPath()))
        df2 = df \
            .withColumn('zLoadDate',
                        F.col('zLoadDate').cast(StringType())
                        ) \
            .withColumn('activity_date',
                        F.col('activity_date').cast(DateType())
                        )
        df2.repartition(2).write.save(
            outputDir,
            mergeSchema='true',
            format='parquet',
            compression='snappy',
            mode='overwrite'
        )
    except:
        print('No files:' + inputDir)


inputDir='/data/zPartKey=1999-01-18/'
outputDir='/data/zPartKey=1999-01-18/'
print(inputDir)
print(outputDir)
df= spark.read.parquet(inputDir) #str(f.getPath()))
print(df.count())
df2 = df \
    .withColumn('zLoadDate',
                F.col('zLoadDate').cast(StringType())
                )
df2.repartition(2).write.save(
    outputDir,
    mergeSchema='true',
    format='parquet',
    compression='snappy',
    mode='overwrite'
)


