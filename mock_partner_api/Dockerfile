FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
ENV MOCK_EFFECTS_FILE=/data/effects.jsonl
VOLUME /data
EXPOSE 9000
# One process only: the counters live in this process.
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "9000"]
