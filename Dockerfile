FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY main.py .

# Environment variables will be set in Cloud Run
ENV PORT=8080

# Use gunicorn for better performance in production
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app