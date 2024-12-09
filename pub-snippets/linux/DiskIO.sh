time fio --name=test --rw=randread --size=256MB --iodepth=1 --numjobs=64 --directory=/tmp/ --bs=4k --group_reporting --direct=1 --time_based --runtime=3600

--directory=/tmp/ depend of test. When i test with NFS mounted in vm, i use mount point directory instad /tmp/.
