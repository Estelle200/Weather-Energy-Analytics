# Weather-Energy-Analytics
Analyzing weather patterns in Hamburg and their impact on energy usage, using a data pipeline and dashboard.

# Data Design
The project uses a simple relational structure to store weather data collected from OpenWeatherMap. Each record represents one weather observation at a specific time in Hamburg.

Fields to extract:

| Field Name        | JSON Path         | Description                                          |
| ----------------- | ----------------- | ---------------------------------------------------- |
| datetime          | `dt`              | UNIX timestamp for when data was recorded            |
| city              | `name`            | Name of the city (e.g., Hamburg)                     |
| temperature       | `main.temp`       | Current temperature in °C                            |
| humidity          | `main.humidity`   | Humidity percentage                                  |
| wind_speed        | `wind.speed`      | Wind speed in m/s                                    |
| weather_condition | `weather[0].main` | Main weather description (Clear, Rain, Clouds, etc.) |


Table Name: weather_data
| Column Name       | Data Type   | Description                                           |
| ----------------- | ----------- | ----------------------------------------------------- |
| datetime          | DATETIME    | Date and time of data recording                       |
| city              | VARCHAR(50) | City name                                             |
| temperature       | FLOAT       | Temperature in Celsius                                |
| humidity          | INT         | Humidity percentage                                   |
| wind_speed        | FLOAT       | Wind speed in meters per second                       |
| weather_condition | VARCHAR(50) | General weather condition (e.g., Rain, Clouds, Clear) |

# Database Setup:
Created a MySQL database (weather_project) and a table (weather_data) to store weather observations. The table includes datetime, temperature, humidity, wind speed, and weather condition fields.

# Methods


# Dashboard


# Goal:
Analyze weather patterns in Hamburg and their potential impact on energy usage, using a data pipeline (Python + MySQL) and a Power BI dashboard.
This project builds a simple data pipeline from OpenWeatherMap to MySQL, with a Power BI dashboard visualizing weather patterns in Hamburg.
