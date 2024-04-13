from datetime import datetime, date
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow import DAG

with DAG(
        dag_id='basic_etl_dag',
        description='A full end-to-end DAG process to extract data from datahub domain names file into a analysable format',
        schedule_interval=None,
        start_date=datetime(2022 ,1, 1),
        catchup=False,
    ) as dag:
    
    extract_task = BashOperator(
        task_id='extract_task',
        bash_command='wget -c https://r2.datahub.io/clt98mhbp000nl708qb1tppbw/master/raw/top-level-domain-names.csv -O /workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-extract-data.csv',
    )
    
    def transformData():
        df = pd.read_csv("workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-extract-data.csv")
        generic_type_df = df[df["Type"] == "generic"]
        today = date.today()
        generic_type_df["Date"] = today.strftime("%Y-%m-%d")
        generic_type_df.to_csv("workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-transform-data.csv", index=False)

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transformData,
        dag=dag
    )
    
    load_task = BashOperator(
        task_id='load_task',
        bash_command='echo -e ".separator ","\n.import --skip 1 /workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-transform-data.csv top_level_domains" | sqlite3 /workspaces/hands-on-introduction-data-engineering-4395021/lab/end-to-end/basic-etl-load-db.db',
        dag=dag,
    )

    extract_task >> transform_task >> load_task


