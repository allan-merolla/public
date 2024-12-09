df= spark.sql("""
SELECT user_id, max(id) as id,max(lo_id) as lo_id,max(status) as status,max(timestamp) as timestamp,max(changed) as changed from vwEvents as A
WHERE A.status == 'completed'
Group By A.user_id
""")
df_col= df.rdd.map(lambda row : row).collect()

missing_users= []
for user in df_col:
    if user[-1] not in df_col:
        missing_users.append(user)
spark.createDataFrame(missing_users).coalesce(0).write.mode('overwrite').option('header',True).format('csv').save('/workspace/missing.csv')
spark.createDataFrame(missing_users).groupBy('changed').agg(F.count('user_id').alias('user_id_count')).sort('user_id_count',ascending=False).show(99) # quick view

print('User Events is subset of User Data list: ' + str(set(df_userevents_col).issubset(df_col)))
print('User Data  is subset of User Events list: ' + str(set(df_col).issubset(df_userevents_col)))
print('Count of unique user activity from Avro: ' + str(len(df_userevents_col)))
print('Count of unique user data: ' + str(len(df_col)))
print('Count of unique user activity missing from enrolments list: ' + str(len(set(df_col).difference(df_userevents_col))))








