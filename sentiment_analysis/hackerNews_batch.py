#!/usr/bin/env python3
"""
Batch collection of Hacker News articles for lambda architecture.
Collects large amounts of historical data, stores in HDFS.

Usage:

  python hackerNews_batch.py --num-stories 25 --output data/

  hdfs dfs -put ./data/*.jsonl /inputs/belincoln_hackerNews/raw/

"""

import json
import sys
import argparse
from pathlib import Path
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time

API_BASE = "https://hacker-news.firebaseio.com/v0"
HEADERS = {"User-Agent": "hn-batch-collector/1.0"}

def fetch_json(url, timeout=30):
    """Fetch JSON from HN API with retry logic."""
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)

def extract_article_text(url, html):
    """Extract article text using BeautifulSoup."""
    try:
        from readability import Document
        doc = Document(html)
        summary_html = doc.summary()
        return BeautifulSoup(summary_html, "lxml").get_text(separator="\n", strip=True)
    except:
        doc = BeautifulSoup(html, "lxml")
        for tag in ("article", "main"):
            el = doc.find(tag)
            if el:
                txt = el.get_text(separator="\n", strip=True)
                if txt:
                    return txt
        ps = doc.find_all("p")
        if len(ps) >= 3:
            return "\n\n".join(p.get_text(strip=True) for p in ps)
        return ""

def fetch_item_with_text(item_id):
    """Fetch HN item and extract article text if URL exists."""
    try:
        item = fetch_json(f"{API_BASE}/item/{item_id}.json")
        if not item:
            return None

        # Only process stories with URLs
        if item.get("type") != "story" or not item.get("url"):
            return None

        url = item["url"]
        print(f"  Fetching article: {url}")

        # Fetch and extract article text
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            text = extract_article_text(url, resp.text)

            # Truncate very long articles
            if len(text) > 100000:
                text = text[:100000]

            item["text"] = text
            item["text_length"] = len(text)
            item["scraped_at"] = datetime.utcnow().isoformat()

            return item

        except Exception as e:
            print(f"  Failed to fetch article: {e}")
            item["text"] = ""
            item["text_length"] = 0
            item["scrape_error"] = str(e)
            return item

    except Exception as e:
        print(f"  Failed to fetch item {item_id}: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Batch collect HN articles")
    parser.add_argument("--num-stories", type=int, default=1000,
                       help="Number of stories to collect")
    parser.add_argument("--output", type=str, default="./hn_batch_output",
                       help="Output directory (local or HDFS path)")
    parser.add_argument("--story-type", type=str, default="top",
                       choices=["top", "new", "best"],
                       help="Type of stories to fetch")
    args = parser.parse_args()

    # Create output directory
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Fetch story IDs
    print(f"Fetching {args.num_stories} {args.story_type} stories...")
    try:
        story_ids = fetch_json(f"{API_BASE}/{args.story_type}stories.json")
    except Exception as e:
        print(f"Failed to fetch story IDs: {e}")
        sys.exit(1)

    # Limit to requested number
    story_ids = story_ids[:args.num_stories]

    # Process each story
    collected = 0
    batch_size = 100
    batch_data = []

    for i, story_id in enumerate(story_ids, 1):
        print(f"[{i}/{len(story_ids)}] Processing story {story_id}")

        item = fetch_item_with_text(story_id)
        if item and item.get("text"):  # Only keep items with text
            batch_data.append(item)
            collected += 1

            # Write batch to file every 100 items
            if len(batch_data) >= batch_size:
                batch_file = out_dir / f"hn_batch_{collected-len(batch_data)+1}_{collected}.jsonl"
                with open(batch_file, "w", encoding="utf-8") as f:
                    for item in batch_data:
                        f.write(json.dumps(item, ensure_ascii=False) + "\n")
                print(f"  Wrote batch to {batch_file}")
                batch_data = []

        # Rate limiting
        time.sleep(0.5)

    # Write remaining items
    if batch_data:
        batch_file = out_dir / f"hn_batch_{collected-len(batch_data)+1}_{collected}.jsonl"
        with open(batch_file, "w", encoding="utf-8") as f:
            for item in batch_data:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
        print(f"  Wrote final batch to {batch_file}")

    print(f"\nCollected {collected} stories with article text")
    print(f"Output directory: {out_dir}")
    print(f"\nNext steps:")
    print(f"  1. Upload to HDFS: hdfs dfs -put {out_dir}/*.jsonl /inputs/belincoln_hackerNews/raw/")
    print(f"  2. Run BatchClassifier.scala to classify articles")

if __name__ == "__main__":
    main()