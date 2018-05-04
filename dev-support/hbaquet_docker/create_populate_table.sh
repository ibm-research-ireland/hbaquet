echo "create 'test','d',{PARQUET_SCHEMA => '{required binary key (UTF8); optional double d:test;}'}" | hbase shell -n
echo "Table test created with parquet schema {required binary key (UTF8); optional double d:test;}"
java -cp "/root/hbaquet-bin/lib/*" org.apache.hadoop.hbase.client.example.MultiThreadedClientExample test 5000 > /dev/null 2>&1
echo "Data populated"