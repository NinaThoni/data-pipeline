from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import psycopg2
import ollama
import traceback

app = FastAPI()

# Load database credentials
DB_URI = os.environ.get("DB_URI")

# Connect to Aiven PostgreSQL
def get_db_connection():
    return psycopg2.connect(DB_URI)

# Allow React frontend to access FastAPI backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Allow only React app
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
        cursor.execute("SELECT text, forecast_date FROM air_quality ORDER BY forecast_date DESC LIMIT 1;")
        result = cursor.fetchone()
        conn.close()

        if not result:
            return {"message": "No air quality data available"}

        air_quality_text, forecast_date = result

        prompt = f"""
        You are a cat and an assistant that provides air quality updates based on historical data. 
        The latest air quality information is: {air_quality_text}.
        The forecast date is {forecast_date}.
        
        As a cat, answer the following question based on this data: {query}
        """
        
        response = ollama.chat(
            model='mistral',
            messages=[{"role": "user", "content": prompt}], 
            stream=False, 
        )

        return response['message']['content']
    
    except Exception as e:
        error_message = traceback.format_exc()
        print(error_message)  # Logs error in Azure
        return {"error": "Something went wrong", "details": str(e)}

@app.get("/test")
async def test():
    return {"status": "API is working"}

