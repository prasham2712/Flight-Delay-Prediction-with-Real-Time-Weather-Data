import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# Caching data load for performance
def load_data():
    flights = pd.read_csv(
        "historical_arrivals_iad.csv",
        parse_dates=["scheduled_arrival", "actual_arrival"]
    )
    obs = pd.read_csv(
        "weather_observation_kiad.csv",
        parse_dates=["timestamp"]
    )
    forecast = pd.read_csv(
        "weather_forecast_kiad.csv",
        parse_dates=["start_time", "end_time"]
    )
    enriched = pd.read_csv(
        "flights_with_both_weather.csv",
        parse_dates=["scheduled_arrival", "actual_arrival", "timestamp"]
    )
    return flights, obs, forecast, enriched

@st.cache_data
def get_data():
    return load_data()

# Main app
def main():
    st.set_page_config(page_title="IAD Arrivals & Weather Dashboard", layout="wide")
    st.title("IAD Arrivals & Weather Dashboard")
    flights, obs, forecast, enriched = get_data()

    # Sidebar filters
    st.sidebar.header("Filters")
    # Date filter on scheduled arrival
    date_min = flights["scheduled_arrival"].min().date()
    date_max = flights["scheduled_arrival"].max().date()
    start_date, end_date = st.sidebar.date_input(
        "Scheduled Arrival Date",
        [date_min, date_max],
        min_value=date_min,
        max_value=date_max
    )
    mask_dates = flights["scheduled_arrival"].dt.date.between(start_date, end_date)
    flights_filtered = flights[mask_dates]
    enriched_filtered = enriched[enriched["scheduled_arrival"].dt.date.between(start_date, end_date)]

    # Airline multiselect
    airlines = sorted(flights_filtered["airline"].dropna().unique())
    selected_airlines = st.sidebar.multiselect("Airlines", airlines, default=airlines)
    if selected_airlines:
        flights_filtered = flights_filtered[flights_filtered["airline"].isin(selected_airlines)]
        enriched_filtered = enriched_filtered[enriched_filtered["airline"].isin(selected_airlines)]

    # Navigation
    view = st.sidebar.radio("View", ["Flights", "Weather Obs", "Forecast", "Enriched"])

    if view == "Flights":
        st.subheader("Flight Statistics")
        col1, col2 = st.columns(2)
        col1.metric("Total Flights", len(flights_filtered))
        col2.metric("Avg Arrival Delay (min)", f"{flights_filtered['arrival_delay'].mean():.1f}")
        st.markdown("---")
        st.subheader("Arrival Delay Distribution")
        fig, ax = plt.subplots()
        ax.hist(flights_filtered['arrival_delay'].dropna(), bins=30)
        ax.set_xlabel("Delay (min)")
        ax.set_ylabel("Count")
        st.pyplot(fig)
        st.markdown("---")
        st.subheader("Flight Data")
        st.dataframe(flights_filtered)

    elif view == "Weather Obs":
        st.subheader("Weather Observations")
        st.dataframe(obs)
        st.markdown("---")
        st.subheader("Temperature Over Time")
        fig, ax = plt.subplots()
        ax.plot(obs['timestamp'], obs['temp_c'])
        ax.set_xlabel("Time")
        ax.set_ylabel("Temp (°C)")
        st.pyplot(fig)

    elif view == "Forecast":
        st.subheader("Weather Forecast")
        st.dataframe(forecast)
        st.markdown("---")
        st.subheader("Forecast Periods")
        fig, ax = plt.subplots()
        ax.hlines(
            y=range(len(forecast)),
            xmin=forecast['start_time'],
            xmax=forecast['end_time'],
            linewidth=4
        )
        ax.set_yticks(range(len(forecast)))
        ax.set_yticklabels(forecast['forecast_type'])
        ax.set_xlabel("Time Window")
        st.pyplot(fig)

    else:  # Enriched
        st.subheader("Flights with Weather Data")
        st.dataframe(enriched_filtered)

if __name__ == "__main__":
    main()
