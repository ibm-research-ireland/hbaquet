# HBaquet

This project adds to HBase native support of the Parquet format. Data in Parquet format
can be consumed from a large range of analytics frameworks like Spark and Flink. In the current
version we support export of HBase data to Parquet format. We plan to enable all operations
(Get,Scan,Put) to use the Parquet format.

## Demo

<a href="https://asciinema.org/a/174641" target="_blank"><u>Right Click+Open in new tab</u></a>

## Getting Started

Use this docker image to get started.

`docker run -it --hostname=docker.local -p 8080:8080 -p 16010:16010 yiannisgkoufas/hbaquet:3.0.0-SNAPSHOT /bin/bash`

Once you are inside the docker container, start Hbase and Spark:

`$ start-hbase.sh`

`$ start-spark.sh`

Visit localhost:16010 and localhost:8080 to confirm that both services are up and running.
Create the Parquet-enabled table and populate it with some test data.

`$ create_populate_table.sh`

Alternatively, if you want to create and populate the table manually, execute the following:

`$ hbase shell`

`hbase(main):001:0> create 'test','d',{PARQUET_SCHEMA => '{required binary key (UTF8); optional double d:test;}'}`

`# Exit the hbase shell`

```$ java -cp "/root/hbaquet-bin/lib/*" org.apache.hadoop.hbase.client.example.MultiThreadedClientExample test 5000```

Now go into hbase shell to export the new table in Parquet format:

`$ hbase shell`

`hbase(main):001:0> exportToParquet 'test'`

Now the data are accessible in Parquet format and can be directly imported into Spark.

`$ spark-shell --master spark://docker.local:7077`

`# In this case we are accessing local files, but the same applies if we were accessing HDFS`

`# In the path displayed below, change "default" with the namespace you are using and "test" with the name of the table you created`

`scala> var data = spark.read.load("/tmp/hbase-root/hbase/data/default/test/*.parquet")`

You can execute the full range of SparkSQL operations on that dataset.
Few examples:

**Count Data**

`scala> data.count`

`res0: Long = 398315`

**Run SQL commands**

`scala> data.registerTempTable("mytable")`

```scala> spark.sql("SELECT * from mytable WHERE `d:test` > 0.3").count()```
`res2: Long = 273480`

## Comments/Feedback

Yiannis Gkoufas: yiannisg@ie.ibm.com
