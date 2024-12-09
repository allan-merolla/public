hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
for i in range(1,32):
    day = ""
    if len(str(i)) < 2:
        day = "0" + str(i)
    else:
        day = str(i)
    pathStr = '/data/zPartKey=2021-02-'+day
    try:
        path = hadoop.fs.Path(pathStr)
        fs.get(conf).listStatus(path)
        for f in fs.get(conf).listStatus(path):
            filepath=(str(f.getPath()))
            if "_SUCCESS" not in filepath:
                #print(filepath)
                df = spark.read.parquet(filepath)

                #print(df.schema['activity_datetime'].dataType)
                print(df.schema['activity_date'].dataType)
                print(df.schema['zLoadDate'].dataType)
    except:
        print('No files:' + pathStr)