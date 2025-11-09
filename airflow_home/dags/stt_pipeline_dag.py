from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import sys

# =============================
# FIX PATH IMPORT (same as before)
# =============================
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(DAG_DIR))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.append(SCRIPTS_DIR)

from process_stt import (
    read_and_clean_csv,           # legacy
    merge_and_clean_data,         # legacy
    aggregate_billing,            # legacy
    save_output,                  # legacy
    init_db,                      # new
    stage_csv_to_db,              # new
    merge_stage_to_unique,        # new
    aggregate_to_billing,         # new
    export_billing_to_csv         # new
)

# =============================
# DEFAULT DAG ARGUMENTS
# =============================
default_args = {
    'owner': 'taufiq',
    'depends_on_past': False,
    'email': ['taufiq@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# =============================
# DAG DEFINITION
# =============================
with DAG(
    dag_id='stt_billing_pipeline',
    default_args=default_args,
    description='Pipeline untuk memproses STT1/STT2 jadi summary billing (SQLite + CSV)',
    schedule=None,  # manual trigger
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=['data-engineer', 'mplus', 'assignment'],
) as dag:

    DATA_DIR = os.path.join(BASE_DIR, 'data')
    OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

    STT1_PATH = os.path.join(DATA_DIR, 'M+ Software Airflow Assignment - STT1.csv')
    STT2_PATH = os.path.join(DATA_DIR, 'M+ Software Airflow Assignment - STT2.csv')
    OUTPUT_PATH = os.path.join(OUTPUT_DIR, 'billing_summary.csv')

    # =============================
    # TASKS
    # =============================

    def t_init_db(**_):
        init_db()

    init_db_task = PythonOperator(
        task_id='init_db',
        python_callable=t_init_db
    )

    def t_stage_stt1(**context):
        df = read_and_clean_csv(STT1_PATH)  # for row count in XCom
        context['ti'].xcom_push(key='stt1_rows', value=len(df))
        stage_csv_to_db(STT1_PATH, "stt1_raw")

    stage_stt1_task = PythonOperator(
        task_id='stage_stt1',
        python_callable=t_stage_stt1
    )

    def t_stage_stt2(**context):
        df = read_and_clean_csv(STT2_PATH)
        context['ti'].xcom_push(key='stt2_rows', value=len(df))
        stage_csv_to_db(STT2_PATH, "stt2_raw")

    stage_stt2_task = PythonOperator(
        task_id='stage_stt2',
        python_callable=t_stage_stt2
    )

    def t_merge(**context):
        merge_stage_to_unique()
        # For visibility in UI
        df_combined = merge_and_clean_data(STT1_PATH, STT2_PATH)
        context['ti'].xcom_push(key='merged_rows', value=len(df_combined))

    merge_task = PythonOperator(
        task_id='merge_stage_to_unique',
        python_callable=t_merge
    )

    def t_aggregate_and_export(**_):
        aggregate_to_billing()
        export_billing_to_csv(OUTPUT_PATH)

    aggregate_task = PythonOperator(
        task_id='aggregate_and_export',
        python_callable=t_aggregate_and_export
    )

    # =============================
    # DEPENDENCIES
    # =============================
    init_db_task >> [stage_stt1_task, stage_stt2_task] >> merge_task >> aggregate_task
