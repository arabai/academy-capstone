from airflow.models import DAG
from airflow.providers.amazon.aws.operators.batch import AwsBatchOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG('my_dag', default_args=default_args,
                    description='A simple DAG',
                    schedule_interval=timedelta(days=1),
                    start_date=datetime(2021, 1, 1),    
                    catchup=False, tags=['example'])


task_1 = AwsBatchOperator(task_id='task_1' job_name='task_1', job_definition='Anas-job', job_queue='academy-capstone-winter-2022-job-queue', overrides={}, dag=dag)