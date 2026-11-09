# One image for all services; the service is chosen with SERVICE_NAME.
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir fastapi==0.162.2 uvicorn==0.54.0 asyncpg==0.32.0 httpx==0.28.1 \
        PyJWT==2.15.0 prometheus-client==0.26.0
COPY common ./common
COPY services ./services
COPY db ./db
RUN useradd -r -u 10001 app && chown -R app /app
USER app
ENV PYTHONUNBUFFERED=1 PORT=8000
CMD ["sh", "-c", "exec uvicorn services.${SERVICE_NAME}.app:app --host 0.0.0.0 --port ${PORT} --no-access-log --log-level warning"]
