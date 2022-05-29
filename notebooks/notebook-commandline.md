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
hdfs dfs -get -f /group/abiskop1/project_data/timetable.csv ../data/timetable.csv
```

```bash
hdfs dfs -get -f /group/abiskop1/project_data/arrivalsRouteStops.csv ../data/arrivalsRouteStops.csv
```

```bash
hdfs dfs -get -f /group/abiskop1/project_data/terminus.csv ../data/terminusRouteStops.csv
```

```bash
hdfs dfs -get -f /group/abiskop1/project_data/departures.csv ../data/departuresRouteStops.csv
```

```bash
hdfs dfs -ls /group/abiskop1/project_data
```

```bash
hdfs dfs -get -f /group/abiskop1/project_data/delay_distrib.csv ../data/delay_distrib.csv
```

```bash
hdfs dfs -ls /group/abiskop1/project_data
```

```bash
hdfs dfs -rm -R /group/abiskop1/project_data/delay_distrib_final.csv
```

```bash
hdfs dfs -ls /group/abiskop1/project_data/delay_distrib_final.csv
```

```bash
hdfs dfs -ls /group/abiskop1/project_data/
```

```bash
hdfs dfs -get /group/abiskop1/project_data/delay_distrib_final.csv ../data/delay_distrib.csv
```

```bash
hdfs dfs -get /group/abiskop1/project_data/route_names.csv ../data/route_names.csv
```

```bash
hdfs dfs -ls /group/abiskop1/project_data
```

```bash
hdfs dfs -get /group/abiskop1/project_data/route_names_types.csv ../data/route_names_types
```

```bash
hdfs dfs -get /group/abiskop1/project_data/timetableFixed.csv/part-00000-438de4fe-4ac2-4fc6-9fe0-1521ebf86d11-c000.csv ../data/timetableF.csv
```

```bash
hdfs dfs -ls /group/abiskop1/project_data/
```

```bash
hdfs dfs -ls /group/abiskop1/project_data/arrivalsRouteStopsFinal.csv
```

```bash
hdfs dfs -get /group/abiskop1/project_data/arrivalsRouteStopsFinal.csv/part-00000-ddc78823-25d9-48d7-b3b1-8ade34f52159-c000.csv ../data/arrivalsFinal.csv
```

```bash
hdfs dfs -ls /group/abiskop1/project_data/terminusFinal.csv
```

```bash
hdfs dfs -get /group/abiskop1/project_data/terminusFinal.csv/part-00000-42723af3-f5dc-4843-bb31-3045ad620923-c000.csv ../data/terminusFinal.csv
```

```bash
hdfs dfs -ls /group/abiskop1/project_data/departuresFinal.csv
```

```bash
hdfs dfs -get /group/abiskop1/project_data/departuresFinal.csv/part-00000-6490bd92-9249-4c34-b2dc-954990e0e3af-c000.csv ../departuresFinal.csv
```

```bash

```
