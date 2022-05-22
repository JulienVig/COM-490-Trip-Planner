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

```
