-- This file will create an ORC table with price data

-- First, map the CSV data we downloaded in Hive
CREATE EXTERNAL TABLE price_csv_qileichen (
    id STRING,
    prices_amountMax FLOAT,
    prices_amountMin FLOAT,
    prices_availability STRING,
    prices_condition STRING,
    prices_currency STRING,
    prices_dateSeen STRING,
    prices_isSale BOOLEAN,
    prices_merchant STRING,
    prices_shipping STRING,
    prices_sourceURLs STRING,
    asins STRING,
    brand STRING,
    categories STRING,
    dateAdded STRING,
    dateUpdated STRING,
    ean STRING,
    imageURLs STRING,
    keys STRING,
    manufacturer STRING,
    manufacturerNumber STRING,
    name STRING,
    primaryCategories STRING,
    sourceURLs STRING,
    upc STRING,
    weight STRING)
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location 'wasbs://hbase-mpcs5301-2024-10-20t23-28-51-804z@hbasempcs5301hdistorage.blob.core.windows.net/inputs/qileichen';


-- Run a test query to make sure the above worked correctly
select id, prices_amountMax, prices_amountMin, prices_merchant, prices_dateSeen from price_csv_qileichen limit 5;

-- Create an ORC table for price data (Note "stored as ORC" at the end)
create table price_history_qileichen(
    id STRING,
    prices_amountMax FLOAT,
    prices_amountMin FLOAT,
    prices_availability STRING,
    prices_condition STRING,
    prices_currency STRING,
    prices_dateSeen STRING,
    prices_isSale BOOLEAN,
    prices_merchant STRING,
    prices_shipping STRING,
    prices_sourceURLs STRING,
    asins STRING,
    brand STRING,
    categories STRING,
    dateAdded STRING,
    dateUpdated STRING,
    ean STRING,
    imageURLs STRING,
    keys STRING,
    manufacturer STRING,
    manufacturerNumber STRING,
    name STRING,
    primaryCategories STRING,
    sourceURLs STRING,
    upc STRING,
    weight STRING)
  stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table price_history_qileichen select * from price_csv_qileichen
where prices_amountMin IS NOT NULL AND prices_amountMax IS NOT NULL AND prices_merchant IS NOT NULL AND prices_merchant != '';