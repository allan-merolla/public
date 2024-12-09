# Create a simple DataFrame, stored into a partition directory
df1 = sqlContext.createDataFrame(sc.parallelize(range(1, 6))\
                                 .map(lambda i: Row(single=i, double=i * 2)))
df1.write.parquet("data/test_table/key=1")
# Create another DataFrame in a new partition directory,
# adding a new column and dropping an existing column
df2 = sqlContext.createDataFrame(sc.parallelize(range(6, 11))
                                 .map(lambda i: Row(single=i, triple=i * 3)))
df2.write.parquet("data/test_table/key=2")
# Read the partitioned table
df3 = sqlContext.read.option("mergeSchema", "true").parquet("data/test_table")
df3.printSchema()
# The final schema consists of all 3 columns in the Parquet files together
# with the partitioning column appeared in the partition directory paths.
# root
# |-- single: int (nullable = true)
# |-- double: int (nullable = true)
# |-- triple: int (nullable = true)
# |-- key : int (nullable = true)
