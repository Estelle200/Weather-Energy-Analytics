# Weather-Energy Analytics Pipeline
Analyzing weather patterns in Hamburg and their impact on energy usage, using a data pipeline and dashboard.

# Tech Stack
Python
MySQL
Power BI
OpenWeather API
SMARD energy data

# Architecture
OpenWeather API
        ↓
Python ETL
        ↓
MySQL Database
        ↓
Power BI Dashboard

# Weather Data Extraction (OpenWeather API)
The weather data is collected from the OpenWeatherMap Current Weather API.
Each API response is parsed and relevant fields are extracted before being stored in the MySQL database.

| Field Name        | JSON Path         | Description                                                           |
| ----------------- | ----------------- | --------------------------------------------------------------------- |
| datetime          | `dt`              | UNIX timestamp representing when the weather observation was recorded |
| city              | `name`            | Name of the city where the observation was recorded (Hamburg)         |
| temperature       | `main.temp`       | Air temperature in °C                                                 |
| humidity          | `main.humidity`   | Relative humidity percentage                                          |
| wind_speed        | `wind.speed`      | Wind speed measured in meters per second                              |
| weather_condition | `weather[0].main` | General weather description (e.g., Clear, Clouds, Rain)               |
| cloudiness        | `clouds.all`      | Cloud cover percentage (0–100%)                                       |

After extraction, the UNIX timestamp (dt) is converted into a UTC datetime format and used to compute an hourly time bucket for aligning weather observations with hourly renewable energy generation data.

Table: weather_data
This table stores weather observations collected from the OpenWeather API.
| Column Name       | Data Type   | Description                                                                        |
| ----------------- | ----------- | ---------------------------------------------------------------------------------- |
| datetime          | DATETIME    | Exact timestamp of the weather observation (UTC)                                   |
| hour_bucket_utc   | DATETIME    | Timestamp rounded down to the hour for aligning with hourly energy generation data |
| city              | VARCHAR(50) | City name where the observation was recorded                                       |
| temperature       | FLOAT       | Air temperature in Celsius                                                         |
| humidity          | INT         | Relative humidity percentage                                                       |
| wind_speed        | FLOAT       | Wind speed in meters per second                                                    |
| weather_condition | VARCHAR(50) | General weather condition (e.g., Clear, Clouds, Rain)                              |
| cloudiness        | INT         | Cloud cover percentage (0–100%)                                                    |
| ingested_at       | TIMESTAMP   | Timestamp indicating when the record was inserted into the database                |

hour_bucket_utc
Renewable generation data from SMARD is reported hourly, while weather observations occur at irregular minutes. This field aligns weather observations with hourly energy data for analysis.

cloudiness
Cloud cover acts as a proxy variable for solar radiation, allowing analysis of how cloud conditions affect photovoltaic generation.

ingested_at
Used for pipeline monitoring and auditing, recording when the ETL process loaded the data.

# Renewable Generation Data
The pipeline retrieves renewable electricity generation data for Germany from the SMARD platform (Bundesnetzagentur). The dataset provides hourly electricity production values for different renewable energy sources.
The data is extracted using SMARD’s public API and transformed into a structured time-series format before being stored in the database.

Fields Extracted:
| Field Name       | Source            | Description                                                           |
| ---------------- | ----------------- | --------------------------------------------------------------------- |
| datetime_utc     | SMARD timestamp   | Hourly timestamp representing the electricity generation interval     |
| region           | SMARD metadata    | Geographic region for the data (Germany – DE)                         |
| wind_onshore_mw  | SMARD series data | Electricity generation from onshore wind turbines in megawatts        |
| wind_offshore_mw | SMARD series data | Electricity generation from offshore wind farms in megawatts          |
| solar_pv_mw      | SMARD series data | Electricity generation from photovoltaic (solar) systems in megawatts |

Renewable Generation Database Table
Table Name: renewable_generation
| Column Name      | Data Type   | Description                                                              |
| ---------------- | ----------- | ------------------------------------------------------------------------ |
| datetime_utc     | DATETIME    | Hourly timestamp of renewable electricity generation                     |
| region           | VARCHAR(20) | Geographic region identifier (DE for Germany)                            |
| wind_onshore_mw  | DOUBLE      | Total electricity generated by onshore wind turbines (MW)                |
| wind_offshore_mw | DOUBLE      | Total electricity generated by offshore wind turbines (MW)               |
| solar_pv_mw      | DOUBLE      | Total electricity generated by photovoltaic systems (MW)                 |
| ingested_at      | TIMESTAMP   | Timestamp when the data was loaded into the database by the ETL pipeline |


Key Characteristics of the Dataset
Hourly Resolution
Generation values represent electricity production aggregated for each hour.
Units (MW)
All generation values are expressed in megawatts (MW), representing instantaneous power production during the hourly interval.

Renewable Categories
Wind Onshore – wind turbines located on land
Wind Offshore – wind farms located in the North Sea and Baltic Sea
Solar PV – photovoltaic electricity generation

This dataset allows analysis of how weather conditions influence renewable energy generation. By aligning hourly renewable generation data with weather observations (via hour_bucket_utc), the project can analyze relationships such as:
wind speed → wind power generation
cloudiness → solar photovoltaic output
time-series variability of renewable energy production

# Example insights
Wind speed vs wind generation
Cloudiness vs solar generation
Time-series renewable patterns

# Dashboard


# Goal:
Analyzing how weather conditions influence renewable energy generation in Germany using automated data pipelines and Power BI dashboards.
