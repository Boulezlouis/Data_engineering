import pandas as pd
import requests
import io
from sqlalchemy import create_engine
import os
import time

# --- CONFIGURATIE ---
DB_URL = os.getenv("DB_URL", "postgresql://admin:password@postgres:5432/weather_db")

# De exacte endpoint uit jouw XML
RMI_URL = "http://opendata.meteo.be/geoserver/aws/wfs"

def run_pipeline():
    engine = create_engine(DB_URL)
    print("🚀 Starten van de AWS 10-min Pipeline gebaseerd op Capabilities...")

    # Parameters exact volgens de XML-specificaties
    params = {
        "service": "WFS",
        "version": "1.1.0",
        "request": "GetFeature",
        "typeName": "aws:aws_10min",  # Exacte naam uit de XML
        "outputFormat": "csv"         # Ondersteund formaat volgens XML
    }

    try:
        print(f"📡 Verbinding maken met: {RMI_URL}")
        # We voegen een timeout toe en een user-agent om blokkades te voorkomen
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(RMI_URL, params=params, headers=headers, timeout=45)

        if response.status_code != 200:
            print(f"❌ Server fout: {response.status_code}")
            print("Response:", response.text[:200])
            return

        # Controleer of we data hebben gekregen of een XML-fout
        if "ows:ExceptionReport" in response.text:
            print("❌ KMI gaf een XML Exception:")
            print(response.text)
            return

        # CSV inladen in Pandas
        df = pd.read_csv(io.BytesIO(response.content), encoding='ISO-8859-1')

        # Kolomnamen opschonen voor SQL
        df.columns = [c.replace('"', '').strip().lower() for c in df.columns]

        # Opslaan in de database
        # Deze tabel bevat nu de 22 kolommen (temp_dry_shelter_avg, etc.)
        df.to_sql('weer_metingen_aws', engine, if_exists='replace', index=False)

        print(f"✅ SUCCES! Er zijn {len(df)} rijen opgeslagen in 'weer_metingen_aws'.")
        print(f"📋 Aantal kolommen gevonden: {len(df.columns)}")
        print(f"🌡️ Kolommen: {list(df.columns[:10])}...")

    except Exception as e:
        print(f"❌ Er is een fout opgetreden: {e}")

if __name__ == "__main__":
    # Wacht even tot de database container klaar is
    print("⏳ Wachten op database...")
    time.sleep(10)
    run_pipeline()
    
    # Houdt container actief voor DBeaver
    while True:
        time.sleep(1000)