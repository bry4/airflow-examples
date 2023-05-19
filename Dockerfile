FROM apache/airflow:latest-python3.10

COPY requirements.txt /requirements.txt
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
