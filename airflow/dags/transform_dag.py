from datetime import datetime, date
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow import DAG

with DAG(
        dag_id='transform_dag',
        description='A DAG to fromsform the extracted data from datahub domain names file',
        schedule_interval=None,
        start_date=datetime(2022 ,1 , 1),
        catchup=False,
    ) as dag:
    
    def transformData():
        df = pd.read_csv("workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/airflow-extract-data.csv")
        generic_type_df = df[df["Type"] == "generic"]
        today = date.today()
        generic_type_df["Date"] = today.strftime("%Y-%m-%d")
        generic_type_df.to_csv("workspaces/hands-on-introduction-data-engineering-4395021/lab/orchestrated/airflow-transform-data.csv", index=False)

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transformData,
        dag=dag
    )

