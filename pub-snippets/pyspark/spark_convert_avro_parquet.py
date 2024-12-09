%%pyspark
from pyspark.sql import functions as F
from pyspark.sql.types import *
avroDf =spark.read.format('avro').load('abfss://data@data.dfs.core.windows.net/data/*/2020/12/*/*/*/*.avro')
#avro->json
jsonRdd = avroDf.select(avroDf.Body.cast("string")).rdd.map(lambda x: x[0])
# context:struct, created:long, injested:long, type:string, uuid:string
jsonSchema = StructType([ \
                         StructField("context", StringType(), True), \
                         StructField("created", LongType(), False), \
                         StructField("injested", LongType(), False), \
                         StructField("type", StringType(), False), \
                         StructField("uuid", StringType(), False) \
                         ])

data = spark.read.schema(jsonSchema).json(jsonRdd)
df_useractivity = data.withColumn("activity_datetime", F.expr("to_date(from_unixtime(int(injested/1000)))")) \
    .withColumn("element_tid", F.expr("get_json_object(context, '$.element.tid')")) \
    .withColumn("user_id", F.expr("get_json_object(context, '$.user.id')")) \
    .withColumn("portal_id", F.expr("get_json_object(context, '$.portal.id')")) \
    .withColumn("anonymous_id", F.expr("get_json_object(context, '$.anonymous.id')")) \
    .withColumn("repo", F.expr("get_json_object(context, '$.application.repo')")) \
    .withColumn("experiment", F.expr("get_json_object(context, '$.experiment.id')")) \
    .withColumn("variant", F.expr("get_json_object(context, '$.variant.id')")) \
    .withColumn("client_ip", F.expr("get_json_object(context, '$.request.clientIP')")) \
    .withColumn("user_agent", F.expr("get_json_object(context, '$.request.userAgent')")).repartition('user_id')
df_useractivity.filter(F.col('user_id') == '123').repartition(1).write.format('parquet').option('header',True).save('/workspace/tasks/1/result')




