-- Create external table that maps to HBase
CREATE EXTERNAL TABLE belincoln_daily_hackernews_hbase (
                                                           day STRING,
                                                           article_count BIGINT,
                                                           avg_upvotes DOUBLE,
                                                           avg_comments DOUBLE
)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
        WITH SERDEPROPERTIES (
        'hbase.columns.mapping' = ':key,metrics:article_count#b,metrics:avg_upvotes,metrics:avg_comments'
        )
    TBLPROPERTIES ('hbase.table.name' = 'belincoln_daily_hackernews');

INSERT OVERWRITE TABLE belincoln_daily_hackernews_hbase
SELECT
    CAST(day AS STRING) as day,
    article_count,
    avg_upvotes,
    avg_comments
FROM belincoln_daily_hackernews;