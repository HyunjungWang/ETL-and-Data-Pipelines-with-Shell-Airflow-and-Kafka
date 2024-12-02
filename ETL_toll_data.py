from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG arguments
default_args = {
    'owner': 'www',
    'start_date': datetime.today(),
    'email': ['www@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# Task 1: Unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/',
    dag=dag,
)

# Task 2: Extract data from CSV
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="""
        awk -F',' 'BEGIN {OFS=","} NR==1 {print "Rowid","Timestamp","Anonymized Vehicle number","Vehicle type"} 
        NR>1 {print $1,$2,$3,$4}' /home/project/airflow/dags/finalassignment/vehicle-data.csv > /home/project/airflow/dags/finalassignment/csv_data.csv
        """,
    dag=dag
)

# Task 3: Extract data from TSV
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="""
        awk -F'\t' 'BEGIN {OFS=","} NR==1 {print "Number of axles","Tollplaza id","Tollplaza code"} 
        NR>1 {print $3,$5,$6}' /home/project/airflow/dags/finalassignment/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/tsv_data.csv
        """,
    dag=dag
)

# Task 4: Extract data from Fixed Width
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="""
        awk '{print substr($0, 10, 5) "," substr($0, 20, 5)}' /home/project/airflow/dags/finalassignment/payment-data.txt > /home/project/airflow/dags/finalassignment/fixed_width_data.csv
        """,
    dag=dag
)

# Task 5: Consolidate data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="""
        echo "Rowid,Timestamp,Anonymized Vehicle number,Vehicle type,Number of axles,Tollplaza id,Tollplaza code,Type of Payment code,Vehicle Code" > /home/project/airflow/dags/finalassignment/extracted_data.csv
        paste -d ',' /home/project/airflow/dags/finalassignment/csv_data.csv \
        /home/project/airflow/dags/finalassignment/tsv_data.csv \
        /home/project/airflow/dags/finalassignment/fixed_width_data.csv | tail -n +2 >> /home/project/airflow/dags/finalassignment/extracted_data.csv
        """,
    dag=dag 
)

# Task 6: Transform data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
        awk -F ',' 'BEGIN {OFS=","} NR==1 {print $0} NR>1 {$4=toupper($4); print $0}' \
        /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/transformed_data.csv
        """,
    dag=dag
)

# Define the task pipeline (task dependencies)
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
