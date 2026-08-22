from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import pendulum

# Definisanje srpske vremenske zone (Beograd / Europe/Belgrade)
local_tz = pendulum.timezone("Europe/Belgrade")

default_args = {
    'owner': 'branislav',
    'depends_on_past': False,
    'email_on_failure': False,
}

# -------------------------------------------------------------
# 1. DAG: Spark Raw-to-Clean obrada u 19:45 (15 do 8 PM)
# -------------------------------------------------------------
with DAG(
    '1_sports_goat_raw_to_clean',
    default_args=default_args,
    description='Spark Raw-to-Clean obrada - pokreće se u 19:45',
    schedule_interval='45 19 * * *',  # 19:45 po beogradskom vremenu
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['sports_goat', 'spark'],
) as dag_raw_to_clean:

    task_raw_to_clean = BashOperator(
        task_id='spark_raw_to_clean_job',
        bash_command='bash /app/run_raw_to_clean.sh ',
    )


# -------------------------------------------------------------
# 2. DAG: Run Batch obrada u 20:00 (8 PM)
# -------------------------------------------------------------
with DAG(
    '2_sports_goat_run_batch',
    default_args=default_args,
    description='Run Batch obrada - pokreće se u 20:00',
    schedule_interval='0 20 * * *',   # 20:00 po beogradskom vremenu
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=['sports_goat', 'batch'],
) as dag_run_batch:

    task_run_batch = BashOperator(
        task_id='ingest_batch_job',
        bash_command='bash /app/run_batch.sh ',
    )