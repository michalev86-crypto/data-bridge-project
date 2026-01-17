FROM python:3.11-slim-bookworm
WORKDIR /app
COPY producer.py .
RUN pip install kafka-python
ENV KAFKA_BROKER="192.168.1.30:9092"
ENV KAFKA_TOPIC="site_sensors"
CMD ["python", "producer.py"]
