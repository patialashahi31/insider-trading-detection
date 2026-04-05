FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY services/dashboard_frontend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/dashboard_frontend/app /app

CMD ["streamlit", "run", "app.py", "--server.address", "0.0.0.0", "--server.port", "8501"]
