-- Create external table that maps discrete daily articles to HBase
-- Row key is the article URL for uniqueness
CREATE EXTERNAL TABLE belincoln_daily_hackernews_discrete_hbase (
  url STRING,
  day STRING,
  author STRING,
  title STRING,
  upvotes BIGINT,
  sentiment_label STRING,
  classification_confidence DOUBLE
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
  'hbase.columns.mapping' = ':key,metrics:day,metrics:author,metrics:title,metrics:upvotes#b,metrics:sentiment_label,metrics:classification_confidence'
)
TBLPROPERTIES ('hbase.table.name' = 'belincoln_daily_hackernews_discrete');

-- Populate the HBase table from the serving-layer discrete table using URL as row key
INSERT OVERWRITE TABLE belincoln_daily_hackernews_discrete_hbase
SELECT
  url AS url,
  CAST(day AS STRING) AS day,
  author,
  title,
  CAST(upvotes AS BIGINT) AS upvotes,
  sentiment_label,
  CAST(classification_confidence AS DOUBLE) AS classification_confidence
FROM belincoln_daily_hackernews_discrete;
