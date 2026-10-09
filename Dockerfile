FROM python:3.12-slim

WORKDIR /app

COPY miniredis ./miniredis

RUN useradd --create-home app && mkdir -p /data

EXPOSE 6379

RUN chmod 755 /data && chown app:app /data

USER app

CMD ["python", "-m", "miniredis", "--host", "0.0.0.0", "--port", "6379", "--aof", "/data/appendonly.aof"]
