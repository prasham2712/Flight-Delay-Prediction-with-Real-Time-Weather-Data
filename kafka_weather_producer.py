# import requests
# import json
# import time
# from datetime import datetime, timedelta
# from kafka import KafkaProducer

# # ——— CONFIG —————————————————————————————————————————————————————
# API_KEY        = "WbUejyd36ZNh7azw1Gh5KMJHy3NREeCA"
# HEADERS        = {"x-apikey": API_KEY}
# BASE_URL       = "https://aeroapi.flightaware.com/aeroapi/"
# AIRPORT_CODE   = "KIAD"
# DAYS_BACK      = 10
# BOOTSTRAP      = "localhost:9092"
# TOPIC_OBS      = f"{AIRPORT_CODE}.weather.observations"
# TOPIC_FC       = f"{AIRPORT_CODE}.weather.forecast"
# MAX_RETRIES    = 5
# RETRY_DELAY    = 60  # seconds

# producer = KafkaProducer(
#     bootstrap_servers=BOOTSTRAP,
#     value_serializer=lambda v: json.dumps(v).encode("utf-8")
# )

# def fetch_with_retry(url, desc):
#     for i in range(1, MAX_RETRIES+1):
#         r = requests.get(url, headers=HEADERS)
#         if r.status_code == 200:
#             return r.json()
#         if r.status_code == 404:
#             print(f"❌ {desc}: 404 not found")
#             return None
#         print(f"⚠ {desc}: {r.status_code}, retry {i}/{MAX_RETRIES} …")
#         time.sleep(RETRY_DELAY)
#     print(f"❌ Giving up on {desc}")
#     return None

# def produce_observations():
#     end_time = datetime.utcnow()
#     for _ in range(DAYS_BACK):
#         ts  = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
#         url = (
#             f"{BASE_URL}airports/{AIRPORT_CODE}/weather/observations"
#             f"?return_nearby_weather=true&timestamp={ts}"
#         )
#         data = fetch_with_retry(url, f"Observations @ {ts}")
#         if not data:
#             break

#         for o in data.get("observations", []):
#             rec = {
#                 "timestamp":       o.get("time"),
#                 "temp_c":          o.get("temp_air"),
#                 "dewpoint_c":      o.get("temp_dewpoint"),
#                 "wind_speed_kt":   o.get("wind_speed"),
#                 "wind_direction":  o.get("wind_direction"),
#                 "visibility_mi":   o.get("visibility"),
#                 "altimeter_inhg":  o.get("pressure"),
#                 "wx_string":       o.get("raw_data"),
#                 "flight_category": o.get("conditions"),
#                 "ceiling":         o.get("ceiling"),
#                 "humidity":        o.get("relative_humidity"),
#                 "cloud_base":      (o.get("clouds") or [{}])[0].get("altitude"),
#                 "cloud_coverage":  (o.get("clouds") or [{}])[0].get("symbol")
#             }
#             producer.send(TOPIC_OBS, rec)

#         end_time -= timedelta(days=1)
#     print("✅ Published observations")

# def produce_forecast():
#     url  = (
#         f"{BASE_URL}airports/{AIRPORT_CODE}/weather/forecast"
#         "?return_nearby_weather=true"
#     )
#     data = fetch_with_retry(url, "Forecast")
#     if not data:
#         return

#     for fc in data.get("forecast", []):
#         rec = {
#             "start":               fc.get("decoded_forecast", {}).get("start"),
#             "end":                 fc.get("decoded_forecast", {}).get("end"),
#             "forecast_text":       fc.get("text"),
#             "temperature":         fc.get("temperature"),
#             "wind_speed":          fc.get("wind_speed"),
#             "wind_direction":      fc.get("wind_direction"),
#             "significant_weather": fc.get("significant_weather")
#         }
#         producer.send(TOPIC_FC, rec)

#     print("✅ Published forecast")

# if __name__ == "__main__":
#     produce_observations()
#     produce_forecast()
#     producer.flush()
#     print("🎉 All weather data sent")
import requests
import json
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

API_KEY       = "WbUejyd36ZNh7azw1Gh5KMJHy3NREeCA"
HEADERS       = {"x-apikey": API_KEY}
BASE_URL      = "https://aeroapi.flightaware.com/aeroapi/"
AIRPORT_CODE  = "KIAD"
DAYS_BACK     = 10
BOOTSTRAP     = "localhost:9092"
TOPIC_OBS     = f"{AIRPORT_CODE}.weather.observations"
TOPIC_FC      = f"{AIRPORT_CODE}.weather.forecast"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def produce_history():
    now = datetime.utcnow()
    for _ in range(DAYS_BACK):
        ts = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Observations
        obs_url = (
            f"{BASE_URL}airports/{AIRPORT_CODE}/weather/observations"
            f"?return_nearby_weather=true&timestamp={ts}"
        )
        resp = requests.get(obs_url, headers=HEADERS)
        if resp.status_code == 200:
            for o in resp.json().get("observations", []):
                producer.send(TOPIC_OBS, {
                    "timestamp":       o.get("time"),
                    "temp_c":          o.get("temp_air"),
                    "dewpoint_c":      o.get("temp_dewpoint"),
                    "wind_speed_kt":   o.get("wind_speed"),
                    "wind_direction":  o.get("wind_direction"),
                    "visibility_mi":   o.get("visibility"),
                    "altimeter_inhg":  o.get("pressure"),
                    "wx_string":       o.get("raw_data"),
                    "flight_category": o.get("conditions"),
                    "ceiling":         o.get("ceiling"),
                    "humidity":        o.get("relative_humidity"),
                    "cloud_base":      (o.get("clouds") or [{}])[0].get("altitude"),
                    "cloud_coverage":  (o.get("clouds") or [{}])[0].get("symbol")
                })

        # Forecast
        fc_url = (
            f"{BASE_URL}airports/{AIRPORT_CODE}/weather/forecast"
            f"?return_nearby_weather=true&timestamp={ts}"
        )
        resp = requests.get(fc_url, headers=HEADERS)
        if resp.status_code == 200:
            for f in resp.json().get("forecast", []):
                producer.send(TOPIC_FC, {
                    "query_time":    ts,
                    "start_time":    f.get("decoded_forecast", {}).get("start"),
                    "end_time":      f.get("decoded_forecast", {}).get("end"),
                    "forecast_text": f.get("text"),
                    "temperature":   f.get("temperature"),
                    "wind_speed":    f.get("wind_speed"),
                    "wind_direction":f.get("wind_direction"),
                    "significant_weather": f.get("significant_weather")
                })

        # flush and then pause 60 seconds before next day
        producer.flush()
        print(f"✅ Published obs+forecast for {ts}, sleeping 60s…")
        time.sleep(60)

        now -= timedelta(days=1)

    print(f"🎉 Done publishing {DAYS_BACK} days of weather history")

if __name__ == "__main__":
    produce_history()
