FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY services/funding_ingestor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/funding_ingestor/app /app

CMD ["python", "main.py"]
