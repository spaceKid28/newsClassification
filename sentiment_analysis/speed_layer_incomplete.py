import json
from kafka import KafkaConsumer
import happybase

HBASE_HOST = "localhost"
HBASE_TABLE = "belincoln_daily_hackernews"
KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
KAFKA_TOPIC = "hn-articles"

def connect_hbase():
    connection = happybase.Connection(HBASE_HOST)
    table = connection.table(HBASE_TABLE)
    return table

def update_daily_metrics(table, day, upvotes, comments):
    row_key = day.encode("utf-8")
    row = table.row(row_key, columns=[b"metrics:article_count",
                                      b"metrics:avg_upvotes",
                                      b"metrics:avg_comments"])

    # get existing values (if any)
    cur_count = int(row.get(b"metrics:article_count", b"0"))
    cur_avg_upvotes = float(row.get(b"metrics:avg_upvotes", b"0.0"))
    cur_avg_comments = float(row.get(b"metrics:avg_comments", b"0.0"))

    new_count = cur_count + 1

    # online average update
    new_avg_upvotes = (cur_avg_upvotes * cur_count + upvotes) / new_count
    new_avg_comments = (cur_avg_comments * cur_count + comments) / new_count

    table.put(
        row_key,
        {
            b"metrics:article_count": str(new_count).encode("utf-8"),
            b"metrics:avg_upvotes": str(new_avg_upvotes).encode("utf-8"),
            b"metrics:avg_comments": str(new_avg_comments).encode("utf-8"),
        },
    )

def main():
    table = connect_hbase()

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="speed-layer-consumer",
    )

    for msg in consumer:
        event = msg.value

        day = event.get("day")
        upvotes = int(event.get("upvotes", 0))
        comments = int(event.get("comments", 0))

        if not day:
            continue

        update_daily_metrics(table, day, upvotes, comments)

if __name__ == "__main__":
    main()
