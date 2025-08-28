#!/usr/bin/env python3
import pandas as pd

def main():
    # 1) Load data
    flights = pd.read_csv(
        "historical_arrivals_iad.csv",
        parse_dates=["scheduled_arrival", "actual_arrival"],
        engine="python", on_bad_lines="skip"
    )
    forecast = pd.read_csv(
        "weather_forecast_kiad.csv",
        parse_dates=["start_time", "end_time", "query_time"],
        engine="python", on_bad_lines="skip"
    )
    obs = pd.read_csv(
        "weather_observation_kiad.csv",
        parse_dates=["timestamp"],
        engine="python", on_bad_lines="skip"
    )

    # 2) Sort each DataFrame by its key for merge_asof
    flights  = flights.sort_values("scheduled_arrival")
    forecast = forecast.sort_values("start_time")
    obs      = obs.sort_values("timestamp")

    # 3) Merge forecasts onto flights:
    #    - For each flight, grab the last TAF whose start_time <= scheduled_arrival
    #    - Tolerance: 12 hours (tweak as needed)
    flights_fc = pd.merge_asof(
        flights,
        forecast,
        left_on="scheduled_arrival",
        right_on="start_time",
        direction="backward",
        tolerance=pd.Timedelta("500m"),
        suffixes=("", "_fc")
    )

    # 4) Merge observations onto that result:
    #    - For each flight, grab the nearest METAR to actual_arrival
    #    - Tolerance: 2 hours (tweak as needed)
    enriched = pd.merge_asof(
        flights_fc.sort_values("actual_arrival"),
        obs,
        left_on="actual_arrival",
        right_on="timestamp",
        direction="nearest",
        tolerance=pd.Timedelta("12h"),
        suffixes=("", "_obs")
    )

    # 5) Report match rates
    total = len(flights)
    n_fc   = enriched["start_time"].notna().sum()
    n_obs  = enriched["timestamp"].notna().sum()
    print(f"Flights: {total}")
    print(f"  Forecast matched:    {n_fc}/{total}")
    print(f"  Observation matched: {n_obs}/{total}")

    # 6) Save enriched DataFrame
    out_file = "flights_with_weather.csv"
    enriched.to_csv(out_file, index=False)
    print(f"Wrote enriched data to {out_file}")

if __name__ == "__main__":
    main()
