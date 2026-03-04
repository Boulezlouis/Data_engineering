import pandas as pd
import requests
import io
from sqlalchemy import create_engine
import os
import time

DB_URL = os.getenv("DB_URL", "postgresql://admin:password@postgres:5432/weather_db")
RMI_URL = "http://opendata.meteo.be/geoserver/aws/wfs"

def run_pipeline():
    try:
        engine = create_engine(DB_URL)
        
        params = {
            "service": "WFS",
            "version": "1.1.0",
            "request": "GetFeature",
            "typeName": "aws:aws_1hour",
            "outputFormat": "csv"
        }

        print(f"📡 Data ophalen van KMI...")
        response = requests.get(RMI_URL, params=params, timeout=45)
        response.raise_for_status()

        # Gebruik latin-1 of utf-8 afhankelijk van de RMI output
        df = pd.read_csv(io.BytesIO(response.content), encoding='ISO-8859-1')

        if df.empty:
            print("⚠️ Geen data ontvangen van de API.")
            return

        # Kolomnamen opschonen: 'aws:temp' -> 'temp'
        df.columns = [c.split(':')[-1].replace('"', '').strip().lower() for c in df.columns]

        print(f"💾 Opslaan in database (tabel: weer_metingen_aws)...")
        df.to_sql('weer_metingen_aws', engine, if_exists='replace', index=False)
        
        print(f"✅ Succes! {len(df)} rijen verwerkt.")

    except Exception as e:
        print(f"❌ Fout: {e}")

if __name__ == "__main__":
    print("⏳ Wachten op database (15s)...")
    time.sleep(15)
    run_pipeline()
    
    print("💤 Pipeline voltooid. Container blijft actief voor inspectie.")
    while True:
        time.sleep(3600)