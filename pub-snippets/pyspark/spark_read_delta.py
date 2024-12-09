%%pyspark
from delta.tables import *


df = DeltaTable.forPath(spark, 'abfss://synapse@storage.dfs.core.windows.net/data/').toDF()
df.show()