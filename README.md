# Gold Price Pipeline & Prediction

An end-to-end data pipeline that ingests macroeconomic data (DXY, Fedfunds, CPI, Treasury Yield), processes it using a medallion architecture (bronze => silver => gold), and trains a machine learning model to predict gold price.

Built with Python, Minio, Docker and Airflow

## #Business Problem

Gold price is influenced by multiple macroeconomic factors such as inflation, USD strength, interest rates, These data sources are often require intergration and transformation before analysis.

This project builds an automated pipeline to:
- Collect data from multiple sources
- Standardize and store them
- Generate features for machine learning
- Predict gold price trends

![Pipeline flow](images/pipelineflow.png)

## #Tech stack

- Language: Python
- Storage: Minio
- Data processing: Pandas
- ML: Scikit-learn
- Infrastructure: Docker, Docker compose
- Orchestration: Apache Airflow

## #Project Structure

gold-price-pred

    ├── dags/                  # Airflow DAGs
    ├── source-code/
    │   ├── ingestion/        # Data ingestion scripts
    │   ├── features/         # Feature engineering
    │   ├── ml/               # Model training
    ├── configs/              # Environment config
    ├── docker-compose.yml
    ├── Dockerfile
    ├── docker_env.env
    ├── .gitignore

## #How to run ?

### 1. Clone repo
git clone ...

### 2. Start services
docker-compose up -d

### 3. Access Airflow UI
http://localhost:8080

### 4. Trigger DAG
(chạy lại pipeline và thêm ảnh trigger UI airflow vào đây)

## #Key learning
- Getting more influence on how to design end-to-end pipeline
- Handling multi-source data ingestion
- Using Minio as datalake
- Cleaning data, prepare features for ML with time series data 
- Orchestrating workflows with Airflow
