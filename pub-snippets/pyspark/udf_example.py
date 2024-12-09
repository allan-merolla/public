from pyspark.sql.types import *
from pyspark.sql import functions as F

## definining the UDF
addDays = F.udf(lambda a,b: datetime.combine(a + timedelta(days=b), datetime.
                                             min.time()),TimestampType())


## using the new addDays UDF function
summary = activity \
    .withColumn( \
                'date', F.to_date(addDays(F.to_date(F.col('activity_datetime')),F.lit(-1))) \
                ) \
    .select( \
            F.col('date'), \
            isAnonymous(F.col('user_id')).alias('anonymous'), \
            checkAnonymous(F.col('user_id'),F.col('device_ip')).alias('user')
            ) \
    .groupBy( \
             'date','user' \
             ).agg( \
                   {'anonymous':'max'}
                   ).withColumnRenamed( \
                                       'max(anonymous)','anonymous' \
                                       ).select('*')
