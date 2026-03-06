import os
import requests
import mysql.connector
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

OWM_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = os.getenv("CITY", "Hamburg")
COUNTRY = os.getenv("COUNTRY_CODE", "DE")

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE", "weather_project"),
}

WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"


def get_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)


def start_run(cur) -> int:
    """Insert a pipeline run record and return run_id."""
    started_at = datetime.utcnow()
    cur.execute(
        """
        INSERT INTO pipeline_runs (started_at, status)
        VALUES (%s, %s)
        """,
        (started_at, "RUNNING"),
    )
    return cur.lastrowid


def finish_run(cur, run_id: int, status: str, rows_affected: int = 0, error_message: str | None = None):
    """Update the pipeline run record with finish status."""
    finished_at = datetime.utcnow()
    cur.execute(
        """
        UPDATE pipeline_runs
        SET finished_at=%s,
            status=%s,
            rows_affected=%s,
            error_message=%s
        WHERE run_id=%s
        """,
        (finished_at, status, rows_affected, error_message, run_id),
    )


def fetch_weather():
    params = {"q": f"{CITY},{COUNTRY}", "appid": OWM_KEY, "units": "metric"}
    r = requests.get(WEATHER_URL, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def transform(data: dict):
    # dt is UNIX timestamp in UTC
    dt_utc = datetime.fromtimestamp(data["dt"], tz=timezone.utc)
    dt_naive_utc = dt_utc.replace(tzinfo=None)  # store UTC as naive DATETIME
    hour_bucket_utc = dt_naive_utc.replace(minute=0, second=0, microsecond=0)
    cloudiness = int(data["clouds"]["all"]) if data.get("clouds") else None

    return (
        dt_naive_utc,
        hour_bucket_utc,
        data.get("name", CITY),
        float(data["main"]["temp"]),
        int(data["main"]["humidity"]),
        float(data["wind"]["speed"]),
        data["weather"][0]["main"] if data.get("weather") else None,
        cloudiness,
    )


def upsert_weather(cur, row) -> int:
    sql = """
    INSERT INTO weather_data (datetime, hour_bucket_utc, city, temperature, humidity, wind_speed, weather_condition, cloudiness)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      hour_bucket_utc=VALUES(hour_bucket_utc),
      temperature=VALUES(temperature),
      humidity=VALUES(humidity),
      wind_speed=VALUES(wind_speed),
      weather_condition=VALUES(weather_condition),
      cloudiness=VALUES(cloudiness);
    """
    cur.execute(sql, row)
    # In MySQL, rowcount is typically:
    # 1 for insert, 2 for update (depends on settings), 0 if no change
    return cur.rowcount


def main():
    if not OWM_KEY:
        raise RuntimeError("Missing OPENWEATHER_API_KEY in .env")

    cnx = get_connection()
    cur = cnx.cursor()

    run_id = None
    try:
        run_id = start_run(cur)
        cnx.commit()

        data = fetch_weather()
        row = transform(data)

        rows_affected = upsert_weather(cur, row)
        cnx.commit()

        finish_run(cur, run_id, status="SUCCESS", rows_affected=rows_affected, error_message=None)
        cnx.commit()

        print(f"SUCCESS run_id={run_id} rows_affected={rows_affected} row={row}")

    except Exception as e:
        # Best effort: record failure (even if something goes wrong mid-way)
        err = f"{type(e).__name__}: {e}"
        try:
            if run_id is None:
                run_id = start_run(cur)  # create a run if failure happened very early
            finish_run(cur, run_id, status="FAILED", rows_affected=0, error_message=err)
            cnx.commit()
        except Exception:
            # If DB is down, we still surface the original error
            pass
        raise

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            cnx.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
