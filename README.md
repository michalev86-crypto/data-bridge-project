# Data Bridge Project 

A distributed data engineering pipeline that simulates IoT sensor data on a **Raspberry Pi**, streams it via **Kafka**, and processes it using **Airflow** on an **Ubuntu** server.

##  System Architecture

1.  **Producer (Raspberry Pi):** Generates crane sensor data (load, noise, temperature) and sends it to a Kafka broker.
2.  **Broker (Ubuntu/Docker):** Managed via Docker Compose, handles the message streams.
3.  **Consumer (Ubuntu/Docker):** Loads data from Kafka into the target storage (Snowflake/MongoDB).
4.  **Orchestration (Airflow):** Manages the ETL DAGs and scheduling.



---

## 📂 Project Structure

* `dags/`: Airflow DAGs for Mongo-to-Snowflake ETL.
* `data_proj/`: Python producer scripts for the Raspberry Pi.
* `kafka-consumer-loader/`: Dockerized Python consumer to load Kafka streams.
* `airflow-custom-image/`: Dockerfile for the customized Airflow environment.
* `docker-compose.yaml`: Infrastructure definition (Kafka, Zookeeper, Airflow, Postgres).

---

##  Getting Started

### 1. Prerequisites
* **Ubuntu Server:** Docker and Docker Compose installed.
* **Raspberry Pi:** Python 3 and `kafka-python` library installed.

### 2. Infrastructure Setup (Ubuntu)
Navigate to the root folder and start the services:
```bash
docker-compose up -d
