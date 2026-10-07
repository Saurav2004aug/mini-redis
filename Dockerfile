FROM python:3.12-slim

WORKDIR /app

COPY miniredis ./miniredis

RUN useradd --create-home app && mkdir -p /data

EXPOSE 6379

CMD ["sh", "-c", "chown -R app:app /data && exec su app -c 'python -m miniredis --host 0.0.0.0 --port 6379 --aof /data/appendonly.aof'"]
