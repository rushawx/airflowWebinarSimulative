import datetime

from airflow.decorators import task, task_group
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
        query += " where updated_at >= now() - interval '1 minute';"

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
            output.append(df[df["city"] == city].to_json(date_format="iso"))

        print(f"Got {len(output)} groups of rows from PostgreSQL. Table: person")

        return output

    @task_group(group_id="transform_data_and_aggregate")
    def transform_data_and_aggregate(data):

        @task
        def transform_data(data):
            import json
            import pandas as pd

            data = json.loads(data)

            df = pd.DataFrame(data)

            print(f"Got {len(df)} rows from PostgreSQL. Table: person")

            df = df.groupby("city").agg({"name": "count"}).reset_index().to_dict(orient="records")

            print(f"Got {len(df)} rows after aggregation. Table: person_count_by_city")

            return df

        @task
        def aggregate_data(data):
            import pandas as pd

            dfs = []

            for sample in data:
                dfs.append(pd.DataFrame.from_dict(sample, orient="index").T)

            df = pd.concat(dfs)

            print(f"Got {len(df)} rows from PostgreSQL. Table: person")

            return df.to_json(date_format="iso")

        t = transform_data(data)

        a = aggregate_data(t)

        return a

    @task
    def load_data_to_ch(data):
        import json
        import pandas as pd
        from clickhouse_driver import Client
        from airflow.hooks.base import BaseHook

        ch_conn = BaseHook.get_connection("ch")

        client = Client(
            host=ch_conn.host,
            port=ch_conn.port,
            database=ch_conn.schema,
            user=ch_conn.login,
            password=ch_conn.password,
        )

        xcom_data = list(data)

        dfs = []

        for sample in xcom_data:
            json_data = json.loads(sample)
            df = pd.DataFrame.from_dict(json_data, orient="index").T
            dfs.append(df)

        df = pd.concat(dfs)

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

    transform = transform_data_and_aggregate.expand(data=extract)

    load = load_data_to_ch(transform)

    sg = say_goodbye()

    ph >> sensor >> extract >> transform >> load >> sg
