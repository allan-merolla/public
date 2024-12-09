Reminders
=========

Great starter guide: https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/ch04.html

Spark is Lazy
Don't Filter before converting to Pandas
Iterate through rows in Spark Driver using PY functions as opposed to Spark functions

Rename columns

Df.columns = ['a','b']
Df.toDF('a','c')

This only impacts metadata, lazy compute means that it won't grab data until a show command is raised or save command

Df.fillna(0) to fill nulls

Always use spark.sql.functions as F

PySpark offers sql support as Spark SQL

Df.createOrReplaceTempVie('foo') #load to temp view
Df2= spark.sql('select * from foo') #return to Dataframe


Standard pyspark libraries
=========
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F


