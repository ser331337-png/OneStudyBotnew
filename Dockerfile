FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 DATA_DIR=/app/data
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY bot.py db.py facts.py ./
RUN mkdir -p /app/data
CMD ["python", "bot.py"]
