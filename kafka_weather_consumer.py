import os
import json
import pandas as pd
from kafka import KafkaConsumer

BOOTSTRAP = ["localhost:9092"]
TOPICS    = [
    "KIAD.weather.observations",
    "KIAD.weather.forecast"
]
CSV_MAP   = {
    "KIAD.weather.observations": "weather_observation_kiad.csv",
    "KIAD.weather.forecast":     "weather_forecast_kiad.csv"
}

consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="weather_consumer_group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Subscribed to:", consumer.subscription())

for msg in consumer:
    topic    = msg.topic
    record   = msg.value
    csv_file = CSV_MAP[topic]
    df       = pd.DataFrame([record])
    header   = not os.path.exists(csv_file)
    df.to_csv(csv_file, mode="a", header=header, index=False)
    print(f"Appended record to {csv_file}")
