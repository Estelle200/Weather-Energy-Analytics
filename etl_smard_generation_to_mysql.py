import os
import requests
import mysql.connector
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE", "weather_project"),
}

REGION = "DE"
RESOLUTION = "hour"

# SMARD filter IDs (generation)
FILTER_WIND_OFFSHORE = 1225
FILTER_WIND_ONSHORE = 4067
FILTER_SOLAR_PV = 4068

SMARD_BASE = "https://www.smard.de/app"


def get_connection():
    return mysql.connector.connect(**MYSQL_CONFIG)


def start_run(cur) -> int:
    started_at = datetime.utcnow()
    cur.execute(
        "INSERT INTO pipeline_runs (started_at, status, pipeline_name) VALUES (%s, %s, %s)",
        (started_at, "RUNNING", "smard_generation"),
    )
    return cur.lastrowid


def finish_run(cur, run_id: int, status: str, rows_affected: int = 0, error_message: str | None = None):
    finished_at = datetime.utcnow()
    cur.execute(
        """
        UPDATE pipeline_runs
        SET finished_at=%s, status=%s, rows_affected=%s, error_message=%s
        WHERE run_id=%s
        """,
        (finished_at, status, rows_affected, error_message, run_id),
    )


def fetch_indices(filter_id: int):
    # Returns available timestamps (ms)
    url = f"{SMARD_BASE}/chart_data/{filter_id}/{REGION}/index_{RESOLUTION}.json"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.json()["timestamps"]


def fetch_timeseries(filter_id: int, timestamp_ms: int):
    # Note: API requires repeating filter + region in the filename (documented as "kaputtes API-Design")
    url = f"{SMARD_BASE}/chart_data/{filter_id}/{REGION}/{filter_id}_{REGION}_{RESOLUTION}_{timestamp_ms}.json"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    # data["series"] is like: [[ms, value], [ms, value], ...]
    return data["series"]


def ms_to_datetime_utc(ms: int) -> datetime:
    # SMARD timestamps are epoch milliseconds; convert to UTC datetime
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).replace(tzinfo=None)


def upsert_generation(cur, dt_utc: datetime, region: str, wind_on: float | None, wind_off: float | None, solar: float | None) -> int:
    sql = """
    INSERT INTO renewable_generation (datetime_utc, region, wind_onshore_mw, wind_offshore_mw, solar_pv_mw)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      wind_onshore_mw=VALUES(wind_onshore_mw),
      wind_offshore_mw=VALUES(wind_offshore_mw),
      solar_pv_mw=VALUES(solar_pv_mw);
    """
    cur.execute(sql, (dt_utc, region, wind_on, wind_off, solar))
    return cur.rowcount


def main():
    cnx = get_connection()
    cur = cnx.cursor()

    run_id = None
    try:
        run_id = start_run(cur)
        cnx.commit()

        # 1) Get "latest" timestamp bundle per filter
        idx_off = fetch_indices(FILTER_WIND_OFFSHORE)
        idx_on = fetch_indices(FILTER_WIND_ONSHORE)
        idx_pv = fetch_indices(FILTER_SOLAR_PV)

        # pick the latest available timestamp for each filter
        ts_off = max(idx_off)
        ts_on = max(idx_on)
        ts_pv = max(idx_pv)

        # 2) Fetch the hourly series bundle for each filter
        series_off = fetch_timeseries(FILTER_WIND_OFFSHORE, ts_off)
        series_on = fetch_timeseries(FILTER_WIND_ONSHORE, ts_on)
        series_pv = fetch_timeseries(FILTER_SOLAR_PV, ts_pv)

        # 3) Merge by timestamp
        wind_off_map = {ms: val for ms, val in series_off}
        wind_on_map = {ms: val for ms, val in series_on}
        solar_map = {ms: val for ms, val in series_pv}

        all_ms = sorted(set(wind_off_map) | set(wind_on_map) | set(solar_map))

        rows_affected_total = 0
        for ms in all_ms:
            wind_on = wind_on_map.get(ms)
            wind_off = wind_off_map.get(ms)
            solar = solar_map.get(ms)

            # Skip hours where SMARD provides no values yet
            if wind_on is None and wind_off is None and solar is None:
                continue

            dt_utc = ms_to_datetime_utc(ms)
            rows_affected_total += upsert_generation(
                cur,
                dt_utc,
                REGION,
                wind_on,
                wind_off,
                solar,
            )

        cnx.commit()
        finish_run(cur, run_id, "SUCCESS", rows_affected_total, None)
        cnx.commit()

        print(f"SUCCESS run_id={run_id} rows_affected={rows_affected_total} hours_loaded={len(all_ms)}")

    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        try:
            if run_id is None:
                run_id = start_run(cur)
            finish_run(cur, run_id, "FAILED", 0, err)
            cnx.commit()
        except Exception:
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