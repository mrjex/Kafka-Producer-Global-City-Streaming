FROM python:3.8-slim

# Install curl for healthcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN set -ex; \
    pip install --no-cache-dir -r requirements.txt

# Set up working directory first
WORKDIR /app/kafka-producer

# Copy files to specific locations
COPY wait-for-it.sh /usr/local/bin/
COPY python-producer.py /app/

# Fix line endings and make script executable
RUN sed -i 's/\r$//' /usr/local/bin/wait-for-it.sh && \
    chmod +x /usr/local/bin/wait-for-it.sh

CMD wait-for-it.sh -s -t 30 $ZOOKEEPER_SERVER -- wait-for-it.sh -s -t 30 $KAFKA_SERVER -- python -u python-producer.py