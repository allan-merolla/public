hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
path = hadoop.fs.Path('/source/tblData/zPartKey=1999-12-11/')
fs.get(conf).listStatus(path)
#print schema first
for f in fs.get(conf).listStatus(path):
    spark.read.parquet(str(f.getPath())).printSchema()
    break


hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
path = hadoop.fs.Path('/source/tblData/zPartKey=1999-12-11/')
fs.get(conf).listStatus(path)
for f in fs.get(conf).listStatus(path):
    df = spark.read.load(
            str(f.getPath()),
            inferSchema='true',
            format='parquet',
            basePath = '/source/stream'
        ).withColumn(
            'activity_date',F.col('activity_date').cast(StringType())
        ).withColumn(
            'user_id',F.col('user_id').cast(StringType())
        ).withColumn(
            'device_browser_version',F.col('device_browser_version').cast(StringType())
        ).withColumn(
            'zLoadDate',F.col('zLoadDate').cast(StringType())
        )
    #UNMANAGED OUTPUT
    df.write.save(
        mergeSchema='true',
        format='parquet',
        compression='snappy',
        mode='append',
        path='/source/tblData/zParkKey=1999-12-11-FIXEDSCHEMA'
        partitionBy='zPartKey'