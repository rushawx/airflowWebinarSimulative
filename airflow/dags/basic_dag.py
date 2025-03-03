import datetime

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task
from airflow.sensors.sql import SqlSensor
from airflow.sensors.external_task_sensor import ExternalTaskSensor


with DAG(
    dag_id="basic_dag",
    schedule="0 18 * * *",
    start_date=datetime.datetime(2025, 1, 1),
    catchup=False,
    tags=["simulative"],
) as dag:

    @task.bash
    def bash_task():
        return "echo https://airflow.apache.org/"

    @task
    def python_task():
        print("Hello Python!")

    bash_task = bash_task()

    python_task = python_task()

    sql_sensor = SqlSensor(
        task_id="sql_check",
        conn_id="postgres",
        sql="""SELECT 1""",
    )

    external_sensor = ExternalTaskSensor(
        task_id="external_check",
        external_dag_id="simulative_example_basic_dag",
        external_task_id="print_hello",
        execution_date_fn=lambda dt: dt,
        dag=dag,
    )

    dummy_task = DummyOperator(task_id="dummy_task")

    bash_task >> python_task >> [sql_sensor, external_sensor] >> dummy_task
