-- Create aggregated view for serving layer
CREATE TABLE belincoln_sentiment_summary
    STORED AS ORC
AS
SELECT
    sentiment_label,
    COUNT(*) as article_count,
    AVG(sentiment_score) as avg_confidence,
    AVG(score) as avg_hn_score,
    AVG(descendants) as avg_comments,
    SUM(score) as total_hn_score,
    COLLECT_LIST(title)[0:10] as sample_titles  -- Top 10 titles
FROM belincoln_hackernews_classified
WHERE sentiment_label IN ('optimistic', 'pessimistic')
GROUP BY sentiment_label;

SELECT
    sentiment_label,
    COUNT(*) as article_count,
    AVG(sentiment_score) as avg_confidence,
    AVG(score) as avg_hn_score,
    AVG(descendants) as avg_comments,
    SUM(score) as total_hn_score
FROM belincoln_hackernews_classified
WHERE sentiment_label IN ('optimistic', 'pessimistic')
GROUP BY sentiment_label;

+------------------+----------------+----------+
|     col_name     |   data_type    | comment  |
+------------------+----------------+----------+
| id               | bigint         |          |
| author           | string         |          |
| date_published   | timestamp      |          |
| title            | string         |          |
| url              | string         |          |
| text             | string         |          |
| score            | bigint         |          |
| descendants      | bigint         |          |
| kids             | array<bigint>  |          |
| sentiment_label  | string         |          |
| sentiment_score  | double         |          |