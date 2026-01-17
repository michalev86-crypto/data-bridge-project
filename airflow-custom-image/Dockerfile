# Use the same base image as specified in your docker-compose.yaml
FROM apache/airflow:2.8.4
#FROM apache/airflow:2.10.3-python3.10
# Install the necessary providers and libraries
RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake \
    apache-airflow-providers-mongo \
    pandas \
    pymongo
