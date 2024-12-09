from pyspark.sql.types import *
from pyspark.sql.functions import *
URI           = sc._gateway.jvm.java.net.URI
Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration
fs = FileSystem.get(URI("hdfs://<namenode_address>:8020"), Configuration())
status = fs.listStatus(Path('<hdfs_directory>'))
filestatus_df=spark.createDataFrame([[str(i.getPath()),i.getModificationTime()/1000] for i in status],["filename","modified_time"]).\
    withColumn("modified_time",to_timestamp(col("modified_time")))
input_df=spark.read.csv("<hdfs_directory>").\
    withColumn("filename",input_file_name())
#join both dataframes on filename to get filetimestamp
df=input_df.join(filestatus_df,['filename'],"left")
