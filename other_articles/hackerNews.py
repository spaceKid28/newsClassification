#!/usr/bin/env python3
"""
Fetch the 3 most recent Hacker News items, save item JSON and extracted article text.

Dependencies (install via pip):
  pip3 install requests beautifulsoup4 lxml readability-lxml newspaper3k

Script prefers readability/newspaper for extraction, falls back to BeautifulSoup heuristics.
"""
from pathlib import Path
import json
import sys
import requests
from bs4 import BeautifulSoup

API_BASE = "https://hacker-news.firebaseio.com/v0"
OUT_DIR = Path.cwd()
NUM = 3
HEADERS = {"User-Agent": "hn-extractor/1.0"}

def fetch_json(url, timeout=30):
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json(), resp.text

def extract_with_readability(html):
    try:
        from readability import Document
    except Exception:
        return None
    doc = Document(html)
    summary_html = doc.summary()
    return BeautifulSoup(summary_html, "lxml").get_text(separator="\n", strip=True)

def extract_with_newspaper(url, html):
    try:
        from newspaper import Article
    except Exception:
        return None
    a = Article(url)
    a.download(input_html=html)
    a.parse()
    return a.text or None

def extract_with_bs4(html):
    doc = BeautifulSoup(html, "lxml")
    # try article/main tags first
    for tag in ("article", "main"):
        el = doc.find(tag)
        if el:
            txt = el.get_text(separator="\n", strip=True)
            if txt:
                return txt
    # fallback to paragraphs if enough
    ps = doc.find_all("p")
    if len(ps) >= 3:
        return "\n\n".join(p.get_text(strip=True) for p in ps)
    # final fallback: body text without script/style
    body = doc.body
    if body:
        for s in body(["script", "style", "noscript", "header", "footer", "aside"]):
            s.decompose()
        return body.get_text(separator="\n", strip=True)
    return ""

def extract_article_text(url, html):
    # try best extractors first
    try:
        txt = extract_with_readability(html)
        if txt:
            return txt
    except Exception:
        pass
    try:
        txt = extract_with_newspaper(url, html)
        if txt:
            return txt
    except Exception:
        pass
    try:
        txt = extract_with_bs4(html)
        if txt:
            return txt
    except Exception:
        pass
    return ""

def main():
    try:
        ids_json, _ = fetch_json(f"{API_BASE}/newstories.json")
    except Exception as e:
        print(f"Failed to fetch newstories: {e}", file=sys.stderr)
        sys.exit(1)

    ids = ids_json[:NUM]
    for _id in ids:
        print(f"Fetching item {_id}")
        try:
            item_json, _ = fetch_json(f"{API_BASE}/item/{_id}.json")
        except Exception as e:
            print(f"  failed to fetch item {_id}: {e}", file=sys.stderr)
            continue

        item_file = OUT_DIR / f"hn_{_id}.json"
        item_file.write_text(json.dumps(item_json, ensure_ascii=False, indent=2), encoding="utf-8")

        url = item_json.get("url")
        if not url:
            print(f"  no url for {_id}, skipping")
            continue

        print(f"  fetching page: {url}")
        text_file = OUT_DIR / f"hn_{_id}_article.txt"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            html = resp.text
        except Exception as e:
            text_file.write_text(f"Failed to fetch page: {e}\n", encoding="utf-8")
            print(f"  failed to fetch page for {_id}: {e}", file=sys.stderr)
            continue

        txt = extract_article_text(url, html)
        if not txt:
            # final fallback: save cleaned HTML->text via bs4 body as last resort
            txt = extract_with_bs4(html) or "No readable text extracted."

        text_file.write_text(txt, encoding="utf-8")
        print(f"  saved text -> {text_file.name}")

    print(f"Saved {len(ids)} Hacker News items (JSON + extracted text) to {OUT_DIR}")

if __name__ == "__main__":
    main()