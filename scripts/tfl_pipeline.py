import html
import requests
import json
import polars as pl
import psycopg2
import re
from datetime import datetime
from typing import Optional, Dict, Any, List
import os

# Load secrets from environment variables
DB_URI = os.getenv("DB_URI")

# Connect to PostgreSQL
conn = psycopg2.connect(DB_URI)
cursor = conn.cursor()

# Extract: Fetch TfL API Data
API_URL = "https://api.tfl.gov.uk/AirQuality/"

def fetch_forecast_data(url: str) -> Optional[Dict[str, Any]]:
    
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()

        return data.get("currentForecast", [])
    
    except requests.exceptions.Timeout:
        print("❌ Error: The API request timed out.")
    except requests.exceptions.HTTPError as http_err:
        print(f"❌ HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"❌ Request error occurred: {req_err}")
    except json.JSONDecodeError:
        print("❌ Error: Failed to parse JSON response.")

forecast_data = fetch_forecast_data(API_URL)

if forecast_data:
    print("✅ API data successfully retrieved!")
else:
    print("❌ Failed to retrieve data.")
    conn.close()
    exit(1)


# Transform: Extract Date from Summary
def clean_text(raw_text):
    """Cleans HTML entities and extra line breaks in the air quality text."""
    if not raw_text:
        return None
    cleaned_text = html.unescape(raw_text)  # Convert HTML entities
    cleaned_text = cleaned_text.replace("<br/>", "\n").strip()  # Replace line breaks
    return cleaned_text

def extract_date(forecast_summary: str) -> datetime:
    match = re.search(r"(\d{1,2} \w+)", forecast_summary)

    if match:
        extracted_date = f"{match.group(1)} {datetime.today().year}"
        return datetime.strptime(extracted_date,  "%d %B %Y")
    return None

def transform_forecast(forecast: List[Dict]) -> pl.DataFrame:
    processed_forecasts = []
    
    for day in forecast:
        processed_forecasts.append({
            "id": day.get("forecastID") + "_" + day.get("$id"),
            "forecast_id": day.get("forecastID"),
            "type": day.get("forecastType"),
            "band": day.get("forecastBand"),
            "summary": clean_text(day.get("forecastSummary")),
            "forecast_date": extract_date(day.get("forecastSummary", "")).date(),
            "end_date": datetime.strptime(day.get("toDate"), "%Y-%m-%dT%H:%M:%SZ").date(),
            "nO2Band": day.get("nO2Band"),
            "o3Band": day.get("o3Band"),
            "pM10Band": day.get("pM10Band"),
            "pM25Band": day.get("pM25Band"),
            "sO2Band": day.get("sO2Band"),
            "text": clean_text(day.get("forecastText")),
        })

    return pl.DataFrame(processed_forecasts)

# Apply Transformations
df = transform_forecast(forecast_data)

# Load: Insert into PostgreSQL
for row in df.iter_rows(named=True):
    cursor.execute("""
        INSERT INTO air_quality (
            id, forecast_id, type, band, summary, forecast_date, end_date, 
            nO2Band, o3Band, pM10Band, pM25Band, sO2Band, text
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE 
        SET 
            (forecast_id, type, band, summary, forecast_date, end_date, nO2Band, o3Band, pM10Band, pM25Band, sO2Band, text) 
            = 
            (EXCLUDED.forecast_id, EXCLUDED.type, EXCLUDED.band, EXCLUDED.summary, EXCLUDED.forecast_date, EXCLUDED.end_date, 
            EXCLUDED.nO2Band, EXCLUDED.o3Band, EXCLUDED.pM10Band, EXCLUDED.pM25Band, EXCLUDED.sO2Band, EXCLUDED.text);

    """, (
        row["id"], row["forecast_id"], row["type"], row["band"], row["summary"],
        row["forecast_date"], row["end_date"], row["nO2Band"], row["o3Band"],
        row["pM10Band"], row["pM25Band"], row["sO2Band"], row["text"]
    ))

conn.commit()
conn.close()

print("✅ Data pipeline execution completed successfully!")








