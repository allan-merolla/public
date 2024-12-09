df_output = df_useractivity \
    .select(
        F.to_date(F.col('activity_datetime')).alias('date'),
        isAnonymous(F.col('user_id').cast(StringType())).alias('anonymous'),
        checkAnonymous(F.col('user_id').cast(StringType()),F.col('device_ip')).alias('user')
    ).groupBy(
        'user'
    ).agg(
        F.min('date').alias("firstdate"),
        F.max('anonymous').alias("anonymous")
    ).union(df_tbldailyfirstvisit).groupBy(
        'user'
    ).agg(
        F.min('firstdate').alias("firstdate"),
        F.max('anonymous').alias("anonymous")
        