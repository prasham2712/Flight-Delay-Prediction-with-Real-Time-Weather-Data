# Flight Delay Prediction with Real-Time Weather Data

## Overview
This project builds a scalable data pipeline that integrates flight data with real-time weather observations and forecasts to predict potential flight delays.  
It leverages Apache Kafka for streaming, PySpark for data processing and machine learning, and Dash/Plotly for interactive visualization.  

The system processes millions of records, achieving:
- 87% prediction accuracy  
- 35% faster runtime compared to baseline implementations  

## Features
- Real-time ingestion of weather and flight data via Kafka producers/consumers  
- Data enrichment combining flight schedules with local weather conditions  
- Feature engineering using VectorAssembler, OneHotEncoder, and StandardScaler  
- Machine Learning pipeline for delay prediction  
- Interactive dashboard for monitoring delays and visualizing flight trends  

## Project Structure
data/
    historical_arrivals_iad.csv     - Historical flight arrivals  
    weather_observation_kiad.csv    - Historical weather observations  
    weather_forecast_kiad.csv       - Weather forecasts  
    flights_with_weather.csv        - Final enriched dataset  

kafka_producer.py                   - Flight data producer  
kafka_consumer.py                   - Flight data consumer  
kafka_weather_producer.py           - Weather data producer  
kafka_weather_consumer.py           - Weather data consumer  
enrich_flights.py                   - Script to join flights with weather  
dashboard.py                        - Dash app for visualization  


### Run Producers and Consumers
python kafka_producer.py  
python kafka_consumer.py  
python kafka_weather_producer.py  
python kafka_weather_consumer.py  

### Enrich Flight Data
python enrich_flights.py  

## Results
- Built a scalable streaming pipeline for flight and weather integration  
- Processed millions of records  
- Achieved 87% accuracy with optimized feature engineering  
- Improved runtime efficiency by 35%  

## Tech Stack
- Apache Kafka – Real-time streaming  
- PySpark – Data processing and ML pipeline  
- Dash/Plotly – Interactive visualization  
- Pandas, NumPy – Data manipulation  
- Sklearn / Spark MLlib – Feature engineering and modeling  

## Future Improvements
- Deploy as a cloud-native pipeline (AWS Kinesis + EMR + S3)  
- Add deep learning models for better accuracy  
- Enable real-time flight delay notifications  

## Author
Prasham Shah  
