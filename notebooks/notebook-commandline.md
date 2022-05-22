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
hdfs dfs -ls /user/benhaim/final-project
```

```bash
hdfs dfs -cp /data/sbb/csv/allstops/stop_locations.csv /user/benhaim/stop_locations.csv
```

```bash
hdfs dfs -chmod 777 /user/benhaim/final-project/stop_walking_time.csv
```

```bash
git lfs track "*.csv"
```

```bash
ls
```

```bash
ls -a
```

```bash
cat ../.gitattributes
```

```bash
mv .gitattributes ..
```

```bash
ls -a
```

```bash
hdfs dfs -cp /user/benhaim/final-project/stop_walking_time.csv /user/benhaim/final-project/stop_walking_time_mine.csv
```

```bash
hdfs dfs -chown benhaim: /user/benhaim/final-project/stop_walking_time.csv
```

```bash
hdfs dfs -ls /user/benhaim/final-project/stop_walking_time
```

```bash
hdfs dfs -get /user/benhaim/final-project/stop_walking_time_mine.csv
```

```bash
hdfs dfs -cp -R /user/benhaim/final-project/stop_walking_time/* /user/benhaim/final-project/stop_walking_time_mine/
```

```bash

```
