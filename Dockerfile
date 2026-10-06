FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scoring ./scoring
COPY batch_score.py .

ENTRYPOINT ["python","batch_score.py"]
