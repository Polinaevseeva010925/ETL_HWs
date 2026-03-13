FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y curl gnupg

USER airflow
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

USER root
RUN chown -R airflow: /opt/airflow
USER airflow
