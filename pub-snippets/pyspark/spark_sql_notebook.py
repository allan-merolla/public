%%pyspark
#collect_set deduplicates items
from pyspark.sql import functions as F
spark.conf.set("spark.sql.shuffle.partitions", "600")
df = spark.sql("""
SELECT A.user_id as user_id, concat_ws(',',collect_set(B.action)) as actions
FROM db.tblUserActivity as A
left join db.tblUserActivity AS B on B.user_id = A.user_id
WHERE A.zPartKey = '2020-12-15' and B.zPartKey = '2020-12-15'
Group By A.user_id
""").repartition('actions')
df_issue = df.groupBy('actions').agg(F.count('user_id'),F.max('user_id').alias('example_user'))
df_issue.repartition(1).write.csv('/results.csv',header=True) #repartition