'use strict';
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = new URL(process.argv[3]); // URL to hadoop cluster, last arg in terminal command
const hbase = require('hbase');
const port = Number(process.argv[2]); // e.g., 3003, comes from terminal command

var hclient = hbase({
    host: url.hostname,
    path: url.pathname ?? "/",
    port: url.port, // http or https defaults
    protocol: url.protocol.slice(0, -1), // Don't want the colon
    encoding: 'latin1',
    auth: process.env.HBASE_AUTH
});

// Decode HBase cell value bytes to UTF-8 string
function cellToString(cellValue) {
    try {
        return Buffer.from(cellValue).toString('utf8');
    } catch (_) {
        return '';
    }
}

// Safely convert HBase row cells to a map: { columnQualifier: valueString }
function rowToStringMap(row) {
    var stats = {};
    if (!row) return stats;

    // Some hbase client calls may give a single cell object instead of an array
    if (!Array.isArray(row)) {
        row = [row];
    }

    row.forEach(function (item) {
        if (!item) return;
        const col = item['column'];
        const val = item['$'];
        if (col != null && val != null) {
            stats[col] = cellToString(val);
        }
    });
    return stats;
}

// Decode an HBase 8-byte big-endian signed long to Number
function decodeHBaseLong(bufOrStr) {
    if (bufOrStr == null) return NaN;
    let buf;
    if (Buffer.isBuffer(bufOrStr)) {
        buf = bufOrStr;
    } else if (typeof bufOrStr === 'string') {
        // hbase client often returns binary cell values as latin1 strings when encoding is set
        buf = Buffer.from(bufOrStr, 'latin1');
    } else {
        return NaN;
    }
    if (buf.length < 8) return NaN;
    try {
        const big = buf.readBigInt64BE(0);
        return Number(big);
    } catch (_) {
        // manual parse
        let hi = buf.readInt32BE(0);
        let lo = buf.readUInt32BE(4);
        return hi * 0x100000000 + lo;
    }
}

function findNumericCell(row, qualifierSuffix) {
    if (!row) return undefined;
    for (const item of row) {
        if (typeof item.column === 'string' && item.column.endsWith(qualifierSuffix)) {
            const val = decodeHBaseLong(item['$']);
            if (!Number.isNaN(val)) return val;
        }
    }
    return undefined;
}

// Helper to convert a discrete-table row into an article object
function discreteRowToArticle(cells) {
    const info = rowToStringMap(cells);

    function pick(col) {
        // discrete table is likely using 'metrics:' prefix as well
        return info[col] ?? info['metrics:' + col] ?? '';
    }

    return {
        day: pick('day'),
        author: pick('author'),
        title: pick('title'),
        upvotes: pick('upvotes'),
        sentiment_label: pick('sentiment_label'),
        classification_confidence: pick('classification_confidence'),
        url: pick('url')
    };
}

app.use(express.static('public'));

// Query daily HackerNews stats by date (YYYY-MM-DD)
app.get('/stats.html', function (req, res) {
    const day = req.query['day'];
    if (!day) {
        return res.status(400).send('Missing required query parameter: day (e.g., 2025-11-27)');
    }
    console.log('Querying daily stats for day:', day);

    hclient.table('belincoln_daily_hackernews').row(day).get(function (err, cells) {
        if (err) {
            console.error('HBase error:', err);
            return res.status(500).send('Error querying HBase');
        }

        const info = rowToStringMap(cells);
        console.log('Row info:', info);

        function pick(key) {
            return info[key] ?? info['metrics:' + key] ?? info['stats:' + key] ?? info['daily:' + key] ?? '';
        }

        const count = findNumericCell(cells, 'article_count');
        const view = {
            day: day || pick('day'),
            article_count: (count !== undefined ? String(count) : pick('article_count')),
            avg_upvotes: pick('avg_upvotes'),
            avg_comments: pick('avg_comments')
        };

        var template = filesystem.readFileSync('result.mustache').toString();
        var html = mustache.render(template, view);
        res.send(html);
    });
});

// New route: list all articles for a given day from belincoln_daily_hackernews_discrete
app.get('/articles.html', function (req, res) {
    const day = req.query['day'];
    if (!day) {
        return res.status(400).send('Missing required query parameter: day (e.g., 2025-11-27)');
    }
    console.log('Querying daily articles for day:', day);

    // Scan a reasonable number of rows and filter by the stored day value in the metrics:day column
    hclient
        .table('belincoln_daily_hackernews_discrete')
        .scan({
            maxVersions: 1,
            // Fetch all needed columns, including day
            columns: [
                'metrics:day',
                'metrics:author',
                'metrics:title',
                'metrics:upvotes',
                'metrics:sentiment_label',
                'metrics:classification_confidence',
                'metrics:url'
            ],
            max: 5000
        }, function (err, rows) {
            if (err) {
                console.error('HBase error (articles scan):', err);
                return res.status(500).send('Error querying HBase for articles');
            }

            if (!rows) {
                console.log('No rows returned from HBase for discrete table (rows is null/undefined)');
                rows = [];
            }

            if (!Array.isArray(rows)) {
                console.log('HBase scan returned a non-array rows object, normalizing to array of one. Raw rows:', rows);
                rows = [rows];
            } else {
                console.log('HBase scan returned', rows.length, 'cell(s). Example first cell:', rows[0]);
            }

            if (rows.length === 0) {
                console.log('No rows returned from HBase for discrete table (rows.length === 0)');
            }

            // Group cells by row key first
            const rowsByKey = {};
            rows.forEach(function (cell) {
                if (!cell || !cell.key) return;
                if (!rowsByKey[cell.key]) rowsByKey[cell.key] = [];
                rowsByKey[cell.key].push(cell);
            });

            const allArticles = Object.keys(rowsByKey).map(function (rowKey, idx) {
                const cellsForRow = rowsByKey[rowKey];
                const info = rowToStringMap(cellsForRow);

                function pick(col) {
                    return info[col] ?? info['metrics:' + col] ?? '';
                }

                // Day and other string fields
                const storedDay = pick('day');
                const author = pick('author');
                const title = pick('title');
                const sentiment = pick('sentiment_label');
                const confidence = pick('classification_confidence');
                const url = pick('url') || rowKey;

                // Decode binary long from metrics:upvotes
                const upvotesVal = findNumericCell(cellsForRow, 'metrics:upvotes');
                const upvotes = (upvotesVal !== undefined && !Number.isNaN(upvotesVal))
                    ? String(upvotesVal)
                    : pick('upvotes');

                const article = {
                    key: rowKey,
                    day: storedDay,
                    author: author,
                    title: title,
                    upvotes: upvotes,
                    sentiment_label: sentiment,
                    classification_confidence: confidence,
                    url: url
                };

                if (idx < 3) {
                    console.log('Article', idx, 'stored day:', article.day,
                        'upvotes (decoded):', upvotesVal,
                        'full article:', article);
                }

                return article;
            });

            // Now filter by the day value from the table, not the row key
            const matchingArticles = allArticles.filter(function (article) {
                return article.day === day;
            });

            console.log('Found', matchingArticles.length, 'articles for day', day);

            const view = {
                title: 'Daily Articles for ' + day,
                day: day,
                articles: true,
                items: matchingArticles
            };

            var template = filesystem.readFileSync('result2.mustache').toString();
            var html = mustache.render(template, view);
            res.send(html);
        });
});

app.listen(port);
