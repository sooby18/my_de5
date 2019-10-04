import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from datetime import datetime, timedelta

args = {
    'owner': 'airflow',
    'start_date': timezone.utcnow()
}

dag = DAG(
    dag_id='maria_sokolova_lab01',
    default_args=args,
    schedule_interval='0 * * * *',
    dagrun_timeout=timedelta(minutes=60),
)

start = DummyOperator(
    task_id='start',
    dag=dag,
)

step0 = DummyOperator(
    task_id='from_kafka_to_logstash',
    dag=dag,
)

step1 = DummyOperator(
    task_id='from_logstash_to_clickhouse',
    dag=dag,
)

start >> step0 >> step1





