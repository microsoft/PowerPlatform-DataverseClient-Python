FROM python:3.11-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir ".[service]"

CMD ["python", "-m", "uvicorn", "examples.host_service.app:app", "--host", "0.0.0.0", "--port", "8080"]