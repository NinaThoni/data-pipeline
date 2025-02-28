from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import os
import psycopg2
import ollama

app = FastAPI()

# Load database credentials
DB_URI = os.getenv("DB_URI")
HF_TOKEN = os.getenv("HF_TOKEN")

# Connect to Aiven PostgreSQL
def get_db_connection():
    return psycopg2.connect(DB_URI)

@app.get("/")
def home():
    return {"message": "London Weather API is running!"}

@app.get("/air_quality_today")
def get_todays_air_quality():
    """Fetches today's air quality summary from the database"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT text FROM air_quality WHERE forecast_date = CURRENT_DATE;")
    result = cursor.fetchone()

    conn.close()

    if result:
        return {"air_quality": result[0]}
    return {"message": "No data available"}

@app.get("/air_quality_tomorrow")
def get_tomorrows_air_quality():
    """Fetches tomorrow's air quality summary from the database"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT text FROM air_quality WHERE forecast_date = CURRENT_DATE + 1;")
    result = cursor.fetchone()

    conn.close()

    if result:
        return {"air_quality": result[0]}
    return {"message": "No data available"}

@app.get("/ask")
async def ask_specific_question(query: str):
    """Uses Mistral to generate an answer based on air quality data"""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get the latest air quality text + forecast date
    cursor.execute("SELECT text, forecast_date FROM air_quality ORDER BY forecast_date DESC LIMIT 1;")
    result = cursor.fetchone()
    conn.close()

    if not result:
        return {"message": "No air quality data available"}

    air_quality_text, forecast_date = result

    # # Format prompt
    prompt = f"""
    You are a cat and an assistant that provides air quality updates based on historical data. 
    The latest air quality information is: {air_quality_text}.
    The forecast date is {forecast_date}.
    
    As a cat, answer the following question based on this data: {query}
    """
    
    def stream():
        for chunk in ollama.chat(
            model='mistral',
            messages=[{"role": "user", "content": prompt}],
            stream=True,
        ):
            yield chunk['message']['content']  # Send response chunks in real-time

    return StreamingResponse(stream(), media_type="text/plain")

