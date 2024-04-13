from datetime import datetime, date
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow import DAG

with DAG(
        dag_id='challenge_dag',
        description='A DAG process to extract S&P 500 data and output aggregate companies in each sector',
        schedule_interval=None,
        start_date=datetime(2022 ,1, 1),
        catchup=False,
    ) as dag:
    
    extract_task = BashOperator(
        task_id='extract_task',
        bash_command='wget -c https://r2.datahub.io/clt989dvv0003l708h2jgh5ef/main/raw/data/constituents.csv -O /workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/challenge-extract-data.csv',
    )
    
    def transformData():
        df = pd.read_csv("workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/challenge-extract-data.csv")
        today = date.today()
        aggregate_df = df.groupby(['GICS Sector']).size().reset_index(name='Count')
        aggregate_df["Date"] = today.strftime("%Y-%m-%d")
        aggregate_df.to_csv("workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/challenge-transform-data.csv", index=False)

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transformData,
        dag=dag
    )
    
    load_task = BashOperator(
        task_id='load_task',
        bash_command='echo -e ".separator ","\n.import --skip 1 /workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/challenge-transform-data.csv sp_500_sector_count" | sqlite3 /workspaces/hands-on-introduction-data-engineering-4395021/lab/challenge/challenge-load-db.db',
        dag=dag,
    )

    extract_task >> transform_task >> load_task


