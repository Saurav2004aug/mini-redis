FROM python:3.12-slim
WORKDIR /app
COPY miniredis ./miniredis
RUN useradd --create-home app && mkdir /data && chown app /data
USER app
EXPOSE 6379
VOLUME /data
CMD ["python", "-m", "miniredis", "--host", "0.0.0.0", "--port", "6379", "--aof", "/data/appendonly.aof"]
