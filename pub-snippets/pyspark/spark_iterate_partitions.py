%%pyspark
spark.conf.set("spark.sql.shuffle.partitions", "600")
dfdates = spark.range(1, 2).withColumn('date', F.to_date(F.lit('1999-01-01'), 'yyyy-MM-dd'))
dfdates_col = dfdates.rdd.map(lambda row : row[1]).collect()
for date in dfdates_col:
    print(str(date))
    df_useractivity = spark.read.load(
        'abfss://data@data.dfs.core.windows.net/activity_datetime='+str(date),
        inferSchema='true',
        format='parquet',
        basePath = 'abfss://data@data.dfs.core.windows.net/'
    )
    df_useractivity \
        .withColumn(
            'zPartKey',F.col('activity_datetime').cast(DateType())
        ).withColumn(
            'activity_date',F.col('activity_datetime').cast(DateType())
        ).withColumn(
            'activity_datetime',F.expr('to_timestamp(from_unixtime(int(injested/1000)))').cast(TimestampType())
        ) \
        .select(
            F.col('uuid').alias('tracking_id').cast(StringType()),
            'activity_date',
            F.col('activity_datetime').alias('activity_datetime').cast(TimestampType()),
            F.col('type').alias('type').cast(StringType()),

            F.expr("get_json_object(context, '$.account.id')").alias('account_id').cast(StringType()),
            F.expr("get_json_object(context, '$.anonymous.id')").alias('anonymous_id').cast(StringType()),
            F.expr("get_json_object(context, '$.portal.id')").alias('portal_id').cast(StringType()),
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
            'zPartKey'
        ).coalesce(1).write.saveAsTable(
            'db.bronze_tblActivity',
            mergeSchema='true',
            format='parquet',
            compression='snappy',
            mode='overwrite',
            path='/bronze/db/tblActivity', #differs with _ to align with container names which cannot have "_"
            partitionBy='zPartKey'
        )