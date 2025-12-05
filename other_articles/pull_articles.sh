#!/bin/bash

API_KEY="48ccaff198ec41f0867c468b0cb2f77b"
OUTPUT_FILE="arabic_articles.json"
LANG="ar"
KEYWORDS="bitcoin"

# Fetch the most recent Arabic articles
curl -X GET "https://newsapi.org/v2/everything?language=${LANG}&sortBy=publishedAt&q=${KEYWORDS}&apiKey=${API_KEY}" \
  -H "Accept: application/json" \
  -o "${OUTPUT_FILE}"

# Check if the request was successful
if [ $? -eq 0 ]; then
    echo "Articles fetched successfully and saved to ${OUTPUT_FILE}"
else
    echo "Failed to fetch articles"
    exit 1
fi