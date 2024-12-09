%%sql
CREATE DATABASE spk_useractivity

df=spark.read.load(
    '/surface/sts/useractivity/tbldailyfirstvisitsingle/',
    inferSchema='true',
    format='parquet',
    basePath = '/surface/sts/useractivity/tbldailyfirstvisitsingle/'
)

df.repartition(1).write.saveAsTable('spk_useractivity.tblDailyFirstVisitTest2',
                                    mergeSchema='true',
                                    format='parquet',
                                    compression='snappy',
                                    mode='overwrite',
                                    path='/surface/sts/useractivity/tblDailyFirstVisitTest2/',
                                    partitionBy='firstdate'
                                    )
df2=spark.read.load(
    '/surface/sts/useractivity/tbldailyfirstvisit_old/*',
    inferSchema='true',
    format='parquet',
    basePath = '/surface/sts/useractivity/tbldailyfirstvisit_old/'
)
df2.repartition(1).write.save(
        mergeSchema='true',
        format='parquet',
        compression='snappy',
        mode='overwrite',
        path='/surface/sts/useractivity/tbldailyfirstvisitsingle/'
        partitionBy='firstdate'
        )

        df2 =spark.read.load(
            '/surface/sts/useractivity/tbldailyuserdistinct_old/*',
            inferSchema='true',
            format='parquet',
            basePath = '/surface/sts/useractivity/tbldailyuserdistinct_old/'
        )

        df2.repartition(1).write.save(
            mergeSchema='true',
            format='parquet',
            compression='snappy',
            mode='overwrite',
            path='/surface/sts/useractivity/tbldailyuserdistinctsingle/'
        )

        df2.repartition(1).write.save(
            mergeSchema='true',
            format='parquet',
            compression='snappy',
            mode='overwrite',
            path='/surface/sts/useractivity/tbldailyuserdistinctparted/',
            partitionBy='date'
        )
        dfdistinct = spark.read.option('inferSchema','true').format('parquet').load('/surface/sts/useractivity/tbldailyuserdistinctsingle/part-00000-ffa62ebc-b745-4d75-b90c-2157ae774a32-c000.snappy.parquet')
        dfdistinct.repartition(1).write.save(
            mergeSchema='true',
            format='parquet',
            compression='snappy',
            mode='append',
            path='/surface/sts/useractivity/tbldailydistinctuser/',
            partitionBy='date'
        )

        df3 = spark.read.option('inferSchema','true').format('parquet').load('/surface/sts/useractivity/tbldailyfirstvisitsingle/part-00000-a33ae558-e1c0-468a-af86-9d77faf7d7b2-c000.snappy.parquet')
        df3.repartition(1).write.save(
            mergeSchema='true',
            format='parquet',
            compression='snappy',
            mode='append',
            path='/surface/sts/useractivity/tbldailyfirstvisit/',
            partitionBy='firstdate'
        )
        spark.read.load('/surface/sts/useractivity/tbldailyfirstvisit/').count()
        spark.read.load('/surface/sts/useractivity/tbldailyfirstvisitparted/').count()
        