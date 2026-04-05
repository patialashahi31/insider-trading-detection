FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY services/dashboard_api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/dashboard_api/app /app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
