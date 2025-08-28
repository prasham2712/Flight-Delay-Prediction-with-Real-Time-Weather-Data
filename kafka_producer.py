import requests
import json
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin
from kafka import KafkaProducer

# ——— CONFIG ———————————————————————————————————————————————
API_KEY       = "WbUejyd36ZNh7azw1Gh5KMJHy3NREeCA"
HEADERS       = {"x-apikey": API_KEY}
BASE          = "https://aeroapi.flightaware.com/aeroapi/"
AIRPORT_CODE  = "KIAD"
DAYS_BACK     = 10
BOOTSTRAP     = 'localhost:9092'
TOPIC         = f"{AIRPORT_CODE}.arrivals"
MAX_RETRIES   = 5
RETRY_DELAY   = 60

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_paginated_arrivals(url):
    cursor = None
    retries = 0
    while True:
        endpoint = cursor if cursor else url
        if cursor and not endpoint.startswith("http"):
            endpoint = urljoin(BASE, cursor.lstrip("/"))
        resp = requests.get(endpoint, headers=HEADERS)
        if resp.status_code == 429 and retries < MAX_RETRIES:
            retries += 1
            time.sleep(RETRY_DELAY)
            continue
        resp.raise_for_status()
        data = resp.json()
        for f in data.get("arrivals", []):
            yield {
                "flight_id":       f.get("fa_flight_id"),
                "origin":          f.get("origin", {}).get("name"),
                "scheduled_arr":   f.get("scheduled_on"),
                "actual_arr":      f.get("actual_on"),
                "airline":         f.get("operator"),
                "status":          f.get("status"),
                "aircraft_type":   f.get("aircraft_type"),
                "departure_delay": f.get("departure_delay"),
                "arrival_delay":   f.get("arrival_delay"),
                "route_distance":  f.get("route_distance")
            }
        cursor = data.get("links", {}).get("next")
        if not cursor:
            break

if __name__ == "__main__":
    # Build URL for the last N days
    end = datetime.utcnow()
    start = end - timedelta(days=DAYS_BACK)
    url = (f"{BASE}airports/{AIRPORT_CODE}/flights/arrivals"
           f"?start={start:%Y-%m-%dT%H:%M:%SZ}&end={end:%Y-%m-%dT%H:%M:%SZ}")

    # Publish each arrival record
    for record in fetch_paginated_arrivals(url):
        producer.send(TOPIC, record)
    producer.flush()
    print("✅ Published all historical arrivals to", TOPIC)
