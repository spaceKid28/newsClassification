# python
# File: `app.py`
# Small FastAPI service that hosts Hugging Face zero-shot pipeline.
# Run: `pip install transformers fastapi uvicorn` then `uvicorn app:app --port 8000`

# # create venv named hackerNews
# python3 -m venv hackerNews
#
# # activate it (Linux)
# source hackerNews/bin/activate


from fastapi import FastAPI
from pydantic import BaseModel
from transformers import pipeline

app = FastAPI()
# This is our zero shot classification model, downloads the model from huggingface
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

# this is what we expect our application to receive
# Our classifier accepts the "text", which is the article text,
# and "candidate_labels" is the list of categories

# recall the only two categories we want to consider are:
# Seq(
#       "The writer expresses optimism about artificial intelligence.",
#       "The writer expresses pessimism about artificial intelligence."
#     )

# Put another way, this defines the expected JSON shape for incoming POST requests
class Payload(BaseModel):
    text: str
    candidate_labels: list[str]
# test
# defines method
@app.post("/classify")
def classify(payload: Payload):
    result = classifier(payload.text, payload.candidate_labels, multi_class=False)
    return result