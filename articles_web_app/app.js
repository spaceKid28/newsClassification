// file: 'newsClassification/articles_web_app/app.js'
const express = require('express');
const mustache = require('mustache');
const fs = require('fs');
const path = require('path');

// Existing imports and app setup...
const app = express();
app.use(express.static(path.join(__dirname, 'public')));

// HBase client setup (example using 'hbase' npm package)
// Ensure 'hbase' is in 'package.json' and installed: `npm install hbase`
const hbase = require('hbase')({
    host: 'localhost',
    port: 8080 // HBase REST gateway port (ensure Stargate/REST is running)
});

// Helper to decode 8-byte big-endian long from Buffer/string
function decodeBinaryLong(val) {
    if (!val) return 0;
    const buf = Buffer.isBuffer(val) ? val : Buffer.from(val, 'latin1');
    if (buf.length < 8) return 0;
    return buf.readBigInt64BE(0);
}

// Existing stats route remains...

// New: daily articles route
app.get('/articles.html', async (req, res) => {
    const day = (req.query.day || '').trim();
    if (!day) {
        res.status(400).send('Missing day');
        return;
    }

    // Build SingleColumnValueFilter on metrics:day equals the requested day
    // Using HBase REST scan with filter string
    const table = hbase.table('belincoln_daily_hackernews_discrete');

    const scannerSpec = {
        filter: `{"type":"SingleColumnValueFilter","op":"EQUAL","family":"metrics","qualifier":"day","comparator":{"type":"BinaryComparator","value":"${day}"}}`,
        columns: [
            'metrics:title',
            'metrics:author',
            'metrics:upvotes',
            'metrics:url',
            'metrics:day'
        ],
        limit: 200
    };

    try {
        table.scan(scannerSpec, (err, rows) => {
            if (err) {
                res.status(500).send('HBase scan error');
                return;
            }
            const items = (rows || []).map(r => {
                const c = r.columns || {};
                const get = (cfq) => c[cfq]?.value || '';
                const upvotes = decodeBinaryLong(get('metrics:upvotes'));
                return {
                    title: get('metrics:title'),
                    author: get('metrics:author'),
                    upvotes: Number(upvotes),
                    url: r.key || get('metrics:url'),
                    day: get('metrics:day')
                };
            });

            const template = fs.readFileSync(path.join(__dirname, 'result.mustache'), 'utf8');
            const html = mustache.render(template, {
                title: `Daily Articles for ${day}`,
                articles: { items }
            });
            res.send(html);
        });
    } catch (e) {
        res.status(500).send('Server error');
    }
});

// Server start (existing)
const port = process.env.PORT || 3000;
app.listen(port, () => {
    console.log(`Server running on http://localhost:${port}`);
});
