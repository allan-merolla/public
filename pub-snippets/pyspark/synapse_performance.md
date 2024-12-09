test 0 = just 1500 partitions = 41min to save
test 1 = executors = 50min + cancelled 1400 / 1868
test 3 = parallelism=500+1500 partitions = 43min
test 4 = parallelism=default+1500 partitions = 36 min - 0 ERRORS!
test 5 = parallelism=default+750 partitions = 36 min
test 6 = shuffle.partitions increase 3000= over 1.5hr and failed actually
test 7 = 1500 partitions + drop the repartition by zPartKey = 1HR 24MIN
test 8 = 750 partitions = 45MIN
test 9 = shuffle.partitions to 1500 and coalesce to 1 without zPartKey column =
test 10 - repartition 500 on zParkey + coalesce 1 = 1.5hr
test 11 - repartition 500 + coalesce 1  = 52 min and still processing and cancelled
Test 12 - default shuffle size + repartition = 1 -> FAILED
Test 13 - 1500 shuffle partitions + colleasce 1 -> FAILED



#default parallism is not applicable ro Dataframes only RDD