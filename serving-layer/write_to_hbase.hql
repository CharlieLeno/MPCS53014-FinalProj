create external table price_hbase_qileichen (
    name_merchant string,
    avg_price float,
    min_price float,
    max_price float)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,price:avg_price#b,price:min_price#b,price:max_price#b')
TBLPROPERTIES ('hbase.table.name' = 'price_hbase_qileichen');


insert overwrite table price_hbase_qileichen
select * from price_qileichen;

scan 'price_hbase_qileichen', {FILTER => "PrefixFilter('Boytone - 2500W 2.1-Ch. Home Theater System - Black Diamond')"}