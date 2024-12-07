create external table latest_price_hbase_qileichen (
    name string,
    merchant float,
    price float)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,name:merchant,name:price#b')
TBLPROPERTIES ('hbase.table.name' = 'latest_price_hbase_qileichen');


spark-submit --java-options "-Dlog4j.configuration=file:///home/sshuser/ss.log4j.properties" --class StreamPrice uber-SpeedLayer-1.0-SNAPSHOT.jar $KAFKABROKERS

spark-submit --verbose \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///home/sshuser/ss.log4j.properties" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///home/sshuser/ss.log4j.properties" \
  --class StreamPrice \
  uber-SpeedLayer-1.0-SNAPSHOT.jar \
  $KAFKABROKERS
