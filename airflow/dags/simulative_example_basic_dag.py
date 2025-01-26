import datetime

from airflow.decorators import task, task_group
from airflow.models.dag import DAG


with DAG(
    dag_id="simulative_example_basic_dag",
    schedule="@daily",
    start_date=datetime.datetime(2025, 1, 1),
    catchup=False,
    tags=["simulative"],
) as dag:

    @task
    def print_hello():
        print("Hello, Simulative!")

    @task_group(group_id="read_data_from_faker_api_and_load_to_pg")
    def read_data_from_faker_api_and_load_to_pg():
        @task
        def read_data_from_faker_api(ti):
            import requests
            from airflow.hooks.base import BaseHook

            faker_api_conn = BaseHook.get_connection("faker")

            response = requests.get(f"http://{faker_api_conn.host}:{faker_api_conn.port}/person")

            if response.status_code == 200:
                data = response.json()
                ti.xcom_push(key="data", value=data)
            else:
                print(f"Error: {response.status_code}")

        @task
        def load_data_to_pg(ti):
            import pandas as pd
            import sqlalchemy
            from airflow.hooks.base import BaseHook

            data = ti.xcom_pull(key="data")
            print(data)

            pg_conn = BaseHook.get_connection("postgres")

            dsn = f"postgresql://{pg_conn.login}:{pg_conn.password}@{pg_conn.host}"
            dsn += f":{pg_conn.port}/{pg_conn.schema}"

            pg_engine = sqlalchemy.create_engine(dsn)

            df = pd.DataFrame.from_dict(data, orient="index").T

            df.to_sql("person", pg_engine, if_exists="append", index=False)
            print(f"Loaded {len(df)} rows to PostgreSQL. Table: person")

        read = read_data_from_faker_api()

        load = load_data_to_pg()

        read >> load

    @task
    def say_goodbye():
        print("Goodbye, Simulative!")

    hello = print_hello()

    main = read_data_from_faker_api_and_load_to_pg()

    goodbye = say_goodbye()

    hello >> main >> goodbye
