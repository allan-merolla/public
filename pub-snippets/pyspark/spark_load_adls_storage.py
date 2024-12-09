hadoop = sc._jvm.org.apache.hadoop
fs = hadoop.fs.FileSystem
conf = hadoop.conf.Configuration()
conf.set("fs.defaultFS", "abfss://data@data.dfs.core.windows.net/")
path = hadoop.fs.Path('/data/data.parquet')x = fs.get(conf).getFileStatus(path)
print(str(x.getPath()))

print(datetime.datetime.fromtimestamp(x.getModificationTime()/1000).strftime('%Y-%m-%dT%H:%M:%S') + 'Z')for f in fs.get(conf).listStatus(path):
    print(str(f.getPath()))
    print(datetime.datetime.fromtimestamp(f.getModificationTime()/1000).strftime('%Y-%m-%d %H:%M:%S'))
    print(datetime.datetime.fromtimestamp(f.getModificationTime()/1000).strftime('%Y-%m-%dT%H:%M:%S') + 'Z')
    break

#Reference: <https://app.slack.com/client/TCM1GJXF0/D01D3DJHZU1/thread/C01D0E2P07N-1614827126.003100>
