FROM python:3.9-slim

WORKDIR /app

# Installeer systeem-tools
RUN apt-get update && apt-get install -y libpq-dev gcc && rm -rf /var/lib/apt/lists/*

# Installeer Python libraries
RUN pip install --no-cache-dir pandas sqlalchemy psycopg2-binary requests

COPY . .

CMD ["python", "loader.py"]