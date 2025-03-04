#!/bin/bash

# Set Airflow Home Directory
export AIRFLOW_HOME=~/airflow  

echo "Initializing Airflow Database..."
airflow db init  # Initialize the database

echo " Creating Airflow Admin User..."
airflow users create \
    --username admin \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@example.com \
    --password airflow

echo "Starting Airflow Webserver on Port 8080..."
airflow webserver --port 8080 -D  # Run in daemon mode

echo "Starting Airflow Scheduler..."
airflow scheduler -D  # Run in daemon mode

echo "Airflow is now running! Access it at http://localhost:8080"
