CC

```bash
hdfs dfs -ls
```

```bash
hdfs dfs -ls /data/sbb/part_orc/stop_times/year=2019/month=01/day=02
```

```bash
hdfs dfs -du -h /data/sbb/part_orc
```

```bash
hdfs dfs -du -h /data/sbb/csv/allstops
```

```bash
hdfs dfs -cat /data/sbb/csv/allstops/stop_locations.csv | head
```

```bash
hdfs dfs -ls /user/benhaim
```

```bash
hdfs dfs -cp /data/sbb/csv/allstops/stop_locations.csv /user/benhaim/stop_locations.csv
```

```bash
hdfs dfs -chmod 777 /user/benhaim/
```

```bash
hdfs dfs -cat /data/sbb/part_orc/transfers/year=2020/month=01/day=01/000000_0 | head
```

```bash
hdfs dfs -mkdir /user/magron/final_project
```

```bash
hdfs dfs -ls /user/magron/final_project/timetableRouteArrival.csv
```

```bash
hdfs dfs -rm -r /user/magron/final_project/timetableRouteArrival.csv
```

```bash

```
