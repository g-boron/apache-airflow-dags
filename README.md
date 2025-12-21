# DAGs Performance Benchmark 📊

### Version 1.3.0

## Project Overview 📘

This project focuses on performing data analysis Python libraries performance tests - measuring
execution time, CPU and RAM usage. Apache Airflow was used to automate testing. 
The project was created as a part of my master's thesis.

## Technologies Used 🛠️

The project is using the following technologies:
- Python 3.11
- Apache Airflow
- Pandas
- Polars
- Dask
- PostgreSQL
- Docker
- PyTest

## Project Structure 🗂️

```
├── dags/
│   ├── dask_benchmark/
│   │   ├── data/
│   │   └── functions/
│   ├── operational_functions/
│   │   ├── dags/
│   │   │    └── dags_parameters.py
│   │   ├── db_engines/
│   │   │    └── postres_engine_builder.py
│   │   ├── files/
│   │   │    └── files_cleaner.py
│   │   └── metrics/
│   │       └── resources.py
│   ├── pandas_benchmark/
│   │   ├── data/
│   │   └── functions/
│   ├── polars_benchmark/
│   │   ├── data/
│   │   └── functions/
│   ├── polars_lazyframe_benchmark/
│   │   ├── data/
│   │   └── functions/
│   ├── tests/
│   ├── dask_benchmark.py
│   ├── dask_benchmark_second.py
│   ├── pandas_benchmark.py
│   ├── pandas_benchmark_second.py
│   ├── polars_benchmark.py
│   ├── polars_benchmark_second.py
│   ├── polars_benchmark.py
│   └── polars_benchmark_second.py
│
├── logs/
│   └── metrics/
├── .gitignore
├── docker-compose.yml
├── README.md
└── requirements.txt
```

Functions that load data into the database and transform data are located in directories 
with names corresponding to the DAG names. Directory `operational_functions` contains functions
that support processes such as creating a database engine, cleaning data directories
and measuring resource usage. Measurements results store into `logs/metrics/<DAG_ID>.csv`.

## Data Description 🗄️

Data used for performing tests is [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (Yellow Trips Data) -
dataset contains taxi rides data in New York. Data was exported into Parquet files. 
In first scenario DAGs is using data from June 2026 (small dataset). 
In second scenario DAGs is using concatenated data from June, July and August 2025 
(large dataset).

## DAGs Pipelines 🔄

DAGs contains tasks:
- clean_data_directory - removing output Parquet files
- Transform_data - transforming raw data (creating daily and payment reports)
- load - loading data into PSQL tables

## Performance Measurement ⏱️

To measure resources, a function decorator was created. Its purpose is to obtain the function's execution time, 
CPU usage, and RAM usage. Results are saved into CSV files (separate file for each DAG). The 
results will be analyzed to assess which Python's library performs best.

## How to Run 🚀

To run project:

1. First create virtual environment using:

    `python -m venv venv`

2. Next install required libraries:

    `pip install -r requirements.txt`

3. Then create Docker containers using:

    `docker compose up airflow-init`
    
    `docker compose up -d`

4. Go to URL `http://localhost:8080/` and login into Apache Airflow using default credentials:
   - username: admin
   - password: admin

5. Create connector to local PostgreSQL database (Admin -> Connections)
6. To schedule tasks create Airflow variable `dags_parameters` with following format:
    
    ```
   {
      "<DAG_ID>": {
         "description":  "<DAG description>",
         "scheduler":    "<CRON scheduler>"
      }
   }
    ```
