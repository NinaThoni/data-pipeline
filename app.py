from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import os
import psycopg2
import traceback
from datetime import date
import logging
import requests

logging.basicConfig(level=logging.WARNING)

app = FastAPI()

# Load database credentials
DB_URI = os.environ.get("DB_URI")
HF_TOKEN = os.environ.get("HF_TOKEN")
HF_MODEL = "mistralai/Mistral-7B-Instruct-v0.3"

# Connect to Aiven PostgreSQL
def get_db_connection():
    return psycopg2.connect(DB_URI)

# Allow React frontend to access FastAPI backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://proud-river-0cfed1503.6.azurestaticapps.net"],  # ✅ Add both local & deployed frontend URLs
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

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

    try:
        """Uses Mistral to generate an answer based on air quality data"""
        conn = get_db_connection()
        cursor = conn.cursor()

        # Get the latest air quality text + forecast date
        cursor.execute("SELECT forecast_date, summary, text FROM air_quality ORDER BY forecast_date DESC LIMIT 10;")
        result = cursor.fetchall()
        conn.close()

        if not result:
            return {"message": "No air quality data available"}

        today = date.today()

        prompt = f"""
        You are a cat that provides London air quality updates based on historical data. 
        The air quality information for the past 10 days is: {result}.
        Today is: {today}
        
        As a cat who knows all this, answer: {query}
        """
        
        headers = {"Authorization": f"Bearer {HF_TOKEN}"}
        payload = {"inputs": prompt}
        response = requests.post(f"https://api-inference.huggingface.co/models/{HF_MODEL}", json=payload, headers=headers)
        response_json = response.json()

        if isinstance(response_json, list) and "generated_text" in response_json[0]:
            answer = response_json[0]["generated_text"].replace(prompt, "").strip()
            print(answer)
            return answer
        
        else:
            return {"error": "Unexpected response format", "raw_response": response_json}

    except Exception as e:
        error_message = traceback.format_exc()
        print(error_message)  # Logs error in Azure
        return {"error": "Something went wrong", "details": str(e)}


