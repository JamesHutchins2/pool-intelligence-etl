FROM apache/airflow:3.1.6

# Switch to root to install system dependencies if needed
USER root

# Install git for any packages that need it
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create directory for pyRealtor module
RUN mkdir -p /opt/airflow/pyRealtor

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# Copy pyRealtor module (will be mounted as volume in docker-compose)
# COPY ./pyRealtor /opt/airflow/pyRealtor
