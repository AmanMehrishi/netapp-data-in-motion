FROM python:3.11-slim

WORKDIR /app


RUN apt-get update && apt-get install -y --no-install-recommends \
    sqlite3 curl ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt


COPY app /app/app
ENV PYTHONPATH=/app

EXPOSE 8000 8050


CMD ["uvicorn","app.services.api:app","--host","0.0.0.0","--port","8000"]
