import datetime

from airflow.decorators import task, task_group
from airflow.models.dag import DAG


with DAG(
    dag_id="simulative_example_dag",
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
            from minio import Minio
            import json
            from io import BytesIO

            minio_conn = BaseHook.get_connection("minio")

            endpoint_url = json.loads(minio_conn.extra)["endpoint_url"]

            minio_client = Minio(
                endpoint_url,
                access_key=minio_conn.login,
                secret_key=minio_conn.password,
                secure=False,
            )

            if not minio_client.bucket_exists("mybucket"):
                minio_client.make_bucket("mybucket")

            faker_api_conn = BaseHook.get_connection("faker")

            response = requests.get(f"http://{faker_api_conn.host}:{faker_api_conn.port}/person")

            if response.status_code == 200:
                data = response.json()
                data_json = json.dumps(data).encode("utf-8")
                data_stream = BytesIO(data_json)
                minio_client.put_object(
                    "mybucket",
                    data["id"],
                    data_stream,
                    len(data_json),
                    content_type="application/json",
                )
                ti.xcom_push(key="mydata", value=data["id"])
            else:
                print(f"Error: {response.status_code}")

        @task
        def load_data_to_pg(ti):
            import pandas as pd
            import sqlalchemy
            from airflow.hooks.base import BaseHook
            from minio import Minio
            import json
            from io import BytesIO

            data_id = ti.xcom_pull(key="mydata")
            print(data_id)

            minio_conn = BaseHook.get_connection("minio")

            endpoint_url = json.loads(minio_conn.extra)["endpoint_url"]

            minio_client = Minio(
                endpoint_url,
                access_key=minio_conn.login,
                secret_key=minio_conn.password,
                secure=False,
            )

            data = minio_client.get_object("mybucket", data_id)

            json_data = json.load(BytesIO(data.read()))

            pg_conn = BaseHook.get_connection("postgres")

            dsn = f"postgresql://{pg_conn.login}:{pg_conn.password}@{pg_conn.host}"
            dsn += f":{pg_conn.port}/{pg_conn.schema}"

            pg_engine = sqlalchemy.create_engine(dsn)

            df = pd.DataFrame.from_dict(json_data, orient="index").T

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
