import datetime

from airflow.decorators import task
from airflow.models.dag import DAG


with DAG(
    dag_id="simulative_example_advanced_dag",
    schedule="@daily",
    start_date=datetime.datetime(2025, 1, 1),
    catchup=False,
    tags=["simulative"],
) as dag:

    @task
    def print_hello():
        print("Hello, Simulative!")

    @task
    def check_pg_for_new_data(ti):
        import time
        import sqlalchemy
        import psycopg2.extras
        from airflow.hooks.base import BaseHook

        query = "select min(updated_at) as dt from public.person"
        query += " where updated_at >= now() - interval '5 minute';"

        pg_conn = BaseHook.get_connection("postgres")

        dsn = f"postgresql://{pg_conn.login}:{pg_conn.password}@{pg_conn.host}"
        dsn += f":{pg_conn.port}/{pg_conn.schema}"

        pg_engine = sqlalchemy.create_engine(dsn)

        conn = pg_engine.raw_connection()

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            while True:
                cur.execute(query)
                data = cur.fetchone()
                if data["dt"] is None:
                    print("No new data")
                    time.sleep(5)
                    continue
                else:
                    print(f"New data: {data['dt']}")
                    ti.xcom_push(key="dt", value=data["dt"])
                    return True

    @task
    def get_data_from_pg(ti):
        import sqlalchemy
        import pandas as pd
        import psycopg2.extras
        from airflow.hooks.base import BaseHook
        from minio import Minio
        import json
        from io import BytesIO

        minio_conn = BaseHook.get_connection("minio")

        endpoint_url = json.loads(minio_conn.extra)["endpoint_url"]

        minio_client = Minio(
            endpoint_url, access_key=minio_conn.login, secret_key=minio_conn.password, secure=False
        )

        if not minio_client.bucket_exists("mybucket"):
            minio_client.make_bucket("mybucket")

        dt = ti.xcom_pull(key="dt")

        pg_conn = BaseHook.get_connection("postgres")

        dsn = f"postgresql://{pg_conn.login}:{pg_conn.password}@{pg_conn.host}"
        dsn += f":{pg_conn.port}/{pg_conn.schema}"

        pg_engine = sqlalchemy.create_engine(dsn)

        conn = pg_engine.raw_connection()

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"select * from public.person where updated_at >= '{dt}';")
            data = cur.fetchall()

        df = pd.DataFrame(data)

        print(f"Got {len(df)} rows from PostgreSQL. Table: person")

        print(df.columns)

        df["id"] = df["id"].astype(str)
        df["city"] = df.apply(lambda row: row["address"].split(",")[0], axis=1)

        output = []

        for city in df["city"].unique():
            output.append(city)
            data = df[df["city"] == city].to_json(date_format="iso")
            data_json = json.dumps(data).encode("utf-8")
            data_stream = BytesIO(data_json)
            minio_client.put_object(
                "mybucket", city, data_stream, len(data_json), content_type="application/json"
            )

        ti.xcom_push(key="mydata", value=output)

        print(f"Got {len(output)} groups of rows from PostgreSQL. Table: person")

        return output

    @task
    def transform_data(input):
        import pandas as pd
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

        item = minio_client.get_object("mybucket", input)

        json_data = json.loads(item.read().decode("utf-8"))
        json_data = json.loads(json_data)

        print(json_data)

        print(type(json_data))

        df = pd.DataFrame.from_dict(json_data, orient="index").T

        print(f"Got {len(df)} rows from PostgreSQL. Table: person")

        df = df.groupby("city").agg({"name": "count"}).reset_index().to_dict(orient="records")

        print(f"Got {len(df)} rows after aggregation. Table: person_count_by_city")

        output = df
        data_json = json.dumps(output).encode("utf-8")
        data_stream = BytesIO(data_json)
        minio_client.put_object(
            "mybucket",
            f"{input}_groupped",
            data_stream,
            len(data_json),
            content_type="application/json",
        )

        return f"{input}_groupped"

    @task
    def aggregate_data(input):
        import pandas as pd
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

        print(input)

        dfs = []

        for city in input:

            local_data = minio_client.get_object("mybucket", city)

            json_data = json.load(BytesIO(local_data.read()))

            df = pd.DataFrame(json_data)

            dfs.append(df)

        df = pd.concat(dfs)

        print(f"Got {len(df)} rows from PostgreSQL. Table: person")

        output = df.to_json(date_format="iso")
        data_json = json.dumps(output).encode("utf-8")
        data_stream = BytesIO(data_json)
        minio_client.put_object(
            "mybucket",
            "final",
            data_stream,
            len(data_json),
            content_type="application/json",
        )

        return "final"

    @task
    def consolidator(input):
        output = []

        for i in range(len(input)):
            output.append(input[i])

        return output

    @task
    def load_data_to_ch(input):
        import pandas as pd
        from clickhouse_driver import Client
        from airflow.hooks.base import BaseHook
        from minio import Minio
        import json

        minio_conn = BaseHook.get_connection("minio")

        endpoint_url = json.loads(minio_conn.extra)["endpoint_url"]

        minio_client = Minio(
            endpoint_url, access_key=minio_conn.login, secret_key=minio_conn.password, secure=False
        )

        ch_conn = BaseHook.get_connection("ch")

        client = Client(
            host=ch_conn.host,
            port=ch_conn.port,
            database=ch_conn.schema,
            user=ch_conn.login,
            password=ch_conn.password,
        )

        data = minio_client.get_object("mybucket", input)
        json_data = json.loads(data.read().decode("utf-8"))
        json_data = json.loads(json_data)
        df = pd.DataFrame(json_data)

        client.insert_dataframe(
            "INSERT INTO person_count_by_city VALUES", df, settings={"use_numpy": True}
        )

        print(f"Loaded {len(df)} rows to ClickHouse. Table: person_count_by_city")

    @task
    def say_goodbye():
        print("Goodbye, Simulative!")

    ph = print_hello()

    sensor = check_pg_for_new_data()

    extract = get_data_from_pg()

    transform = transform_data.expand(input=extract)

    consolidated_data = consolidator(transform)

    aggregate = aggregate_data(consolidated_data)

    load = load_data_to_ch(aggregate)

    sg = say_goodbye()

    ph >> sensor >> extract >> transform >> aggregate >> load >> sg
