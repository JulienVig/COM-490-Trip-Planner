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

## Retriving a file in remote hdfs 

```bash
hdfs dfs -copyToLocal -f /user/magron/sbb_prep_data/timetable.csv ../data/timetable1.csv
```

```bash
hdfs dfs -chown -R magron /user/magron/final_project/
```

```bash
hdfs dfs -cp /user/magron/final_project/timetables.csv /user/magron/final_project/timetable.csv
```

```bash
hdfs dfs -ls /user/magron/final_project
```

```bash
ls -l ../data/timetable.csv/timetable.csv
```

```bash
hdfs dfs -cat /user/magron/final_project/timetable.csv | head
```

```bash
hdfs dfs -ls /data/magron/final_project/time
```

```bash
hdfs dfs -ls /user/magron/final_project/
```

```bash
hdfs dfs -chown magron /user/magron/final_project/timetableFinalMAGRON.csv
```

```bash
hdfs dfs -cp /user/magron/final_project/timetableFinal.csv /user/magron/final_project/timetableFinalMAGRON.csv
```

```bash
hdfs dfs -cp magron /user/magron/data_project /user/magron/data_projectMAG
```

```bash
hdfs dfs -chmod o+r /user/magron/data_project
```

```bash
hdfs dfs -mkdir /user/magron/sbb_prep_data
```

```bash
hdfs dfs -chown magron /user/magron/sbb_prep_data/timetable.csv
```

```bash
hdfs dfs -ls /user/magron/sbb_prep_data/
```

```bash

```
