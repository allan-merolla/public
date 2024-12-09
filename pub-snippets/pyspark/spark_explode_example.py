%%pyspark
#session config
from pyspark.sql.types import ArrayType, IntegerType
from pyspark.sql import functions as F
spark.conf.set("spark.sql.shuffle.partitions", "600")


#collect_set deduplicates items
df = spark.sql("""
SELECT A.user_id as user_id, concat_ws(',',collect_set(B.action)) as actions
FROM db.activity as A
left join db.activity AS B on B.user_id = A.user_id
WHERE A.zPartKey = '1999-12-15' and B.zPartKey = '1999-12-15'
Group By A.user_id
""").repartition('actions')

#filter preenricher data with
df_issue = df.groupBy('actions').agg(F.count('user_id').alias('user_id_count'),F.max('user_id').alias('example_user'))
listofactions = spark.sql("""
SELECT action from tblActions
""").select('action').rdd.map(lambda row : row[0]).collect()
print(listofactions)

df_explode = df_issue.withColumn('explode',F.explode(F.split(F.col('actions'),','))).withColumn('explodematch',F.when(F.col('explode').isin(listofactions),1).otherwise(0))
df_explode.repartition(1).write.csv('/data.csv',header=True)
#df.show(20,False)

df_output = df_explode.groupBy('actions').agg(F.max('user_id_count').alias('user_id_count'),F.max('example_user').alias('example_user'),F.max('explodematch').alias('explodematch')).filter(F.col('explodematch') ==0)
df_output.repartition(1).write.csv('/data.csv',header=True)
#df.show(20,False)
