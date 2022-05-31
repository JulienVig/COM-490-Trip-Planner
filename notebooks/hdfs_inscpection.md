<!-- #region -->
## Retrieving large files stored in the HDFS


We processed files in Spark and stored the processed version back in HDFS. We now have to retrieve then to store the csv n the `data/` folder.


#### Data folder :
<!-- #endregion -->

```bash
hdfs dfs -ls /group/abiskop1/project_data 
```

#### Items to retrieve :

```bash
hdfs dfs -ls /group/abiskop1/project_data/routestops
```

```bash
hdfs dfs -ls /group/abiskop1/project_data/timetableRefacFinal
```

## Fetching the files 

```bash
hdfs dfs -get /group/abiskop1/project_data/routestops/part-00000-08d179f8-4372-4816-944a-ac5bb0a1bd95-c000.csv ../data/routestops.csv
```

```bash
hdfs dfs -get /group/abiskop1/project_data/timetableRefacFinal/part-00000-9b1fbd49-6237-4ffe-bb0e-8322fa5e2369-c000.csv ../data/timetable.csv
```
