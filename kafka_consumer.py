import os
import json
import pandas as pd
from kafka import KafkaConsumer

BOOTSTRAP = ['localhost:9092']
TOPIC     = "KIAD.arrivals"
CSV_FILE  = "historical_arrivals_iad.csv"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset='earliest',   # start from beginning of topic
    enable_auto_commit=True,
    group_id='arrival_consumer',    # any unique group name
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Subscribed to:", consumer.subscription())

for msg in consumer:
    record = msg.value
    df = pd.DataFrame([record])
    write_header = not os.path.exists(CSV_FILE)
    df.to_csv(CSV_FILE, mode='a', header=write_header, index=False)
    print(f"Appended flight {record.get('flight_id')} to {CSV_FILE}")
