from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'me',
    'email': 'me@unknown.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.today()
}

dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily',  # Daily run
    catchup=False
)

unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project',
    dag=dag
)

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="cut -d',' -f1,2,3,4 /home/project/airflow/dags/finalassignment/staging/._vehicle-data.csv > /home/project/airflow/dags/finalassignment/results/csv_data.csv",
    dag=dag
)

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="cut -f5,6,7 /home/project/airflow/dags/finalassignment/staging/._tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/results/tsv_data.csv",
    dag=dag
)

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="cut -c59-61,63-67 /home/project/airflow/dags/finalassignment/staging/payment-data.txt > /home/project/airflow/dags/finalassignment/results/fixed_width_data.csv",
    dag=dag
)

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="paste -d',' /home/project/csv_data.csv /home/project/tsv_data.csv /home/project/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/results/extracted_data.csv",
    dag=dag
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command="awk -F',' 'BEGIN {OFS=\",\"} { $4=toupper($4); print }' /home/project/extracted_data.csv > /home/project/airflow/dags/finalassignment/results/transformed_data.csv",
    dag=dag
)


unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
