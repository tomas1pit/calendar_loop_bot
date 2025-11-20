FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DEFAULT_TIMEOUT=100

WORKDIR /app

# Зависимости для cryptography и caldav
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libssl-dev \
        libffi-dev \
        python3-dev \
        cargo \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./

# Без апгрейда pip
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py ./

ENV DB_PATH=/app/calendar_bot.db
ENV TZ=Europe/Moscow

EXPOSE 8000

CMD ["python", "bot.py"]