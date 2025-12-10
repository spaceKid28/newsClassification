-- Create daily aggregation table
CREATE TABLE belincoln_daily_hackernews
    STORED AS ORC
AS
SELECT
    DATE(date_published) as day,
    COUNT(*) as article_count,
    AVG(score) as avg_upvotes,
    AVG(descendants) as avg_comments
FROM belincoln_hackernews_classified
WHERE date_published IS NOT NULL
GROUP BY
    DATE(date_published)
ORDER BY day DESC;

-- SELECT * FROM belincoln_daily_hackernews LIMIT 20;

-- create daily article table
CREATE TABLE belincoln_daily_hackernews_discrete
    STORED AS ORC
AS
SELECT
    DATE(date_published) as day,
    author,
    title,
    score as upvotes,
    sentiment_label,
    sentiment_score as classification_confidence,
    url
FROM belincoln_hackernews_classified
WHERE date_published IS NOT NULL
ORDER BY day DESC;
