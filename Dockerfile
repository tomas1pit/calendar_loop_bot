FROM python:3.12-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DEFAULT_TIMEOUT=120

WORKDIR /app

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py ./

ENV DB_PATH=/app/calendar_bot.db
ENV TZ=Europe/Moscow

EXPOSE 8000

CMD ["python", "bot.py"]