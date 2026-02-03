FROM python:3.11-slim

# Prevent Python from writing .pyc files and enable unbuffered logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Default port used by many platforms (Cloud Run sets $PORT)
ENV PORT=8080

CMD ["bash", "-lc", "uvicorn api:app --host 0.0.0.0 --port ${PORT}"]
