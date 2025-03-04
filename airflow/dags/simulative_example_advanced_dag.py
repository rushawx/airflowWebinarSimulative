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
        import psycopg2.extras
        from utils import get_pg_engine

        query = "select min(updated_at) as dt from public.person"
        query += " where updated_at >= now() - interval '5 minute';"

        pg_engine = get_pg_engine()

        conn = pg_engine.raw_connection()

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            while True:
                cur.execute(query)
                data = cur.fetchone()
                if data.get("dt") is None:
                    print("No new data")
                    time.sleep(5)
                    continue
                else:
                    print(f"New data: {data['dt']}")
                    ti.xcom_push(key="dt", value=data["dt"])
                    return True

    @task
    def get_data_from_pg(ti):
        import pandas as pd
        import psycopg2.extras
        from utils import get_minio_client, write_data_to_minio, get_pg_engine

        minio_client = get_minio_client()

        if not minio_client.bucket_exists("mybucket"):
            minio_client.make_bucket("mybucket")

        dt = ti.xcom_pull(key="dt")

        pg_engine = get_pg_engine()

        conn = pg_engine.raw_connection()

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"select * from public.person where updated_at >= '{dt}';")
            data = cur.fetchall()

        df = pd.DataFrame(data)

        print(f"Got {len(df)} rows from PostgreSQL. Table: person")

        df["id"] = df["id"].astype(str)
        df["city"] = df.apply(lambda row: row["address"].split(",")[0], axis=1)

        output = []

        for city in df["city"].unique():
            output.append(city)
            data = df[df["city"] == city].to_json(date_format="iso")
            write_data_to_minio(minio_client, data, city)

        ti.xcom_push(key="mydata", value=output)

        print(f"Got {len(output)} groups of rows from PostgreSQL. Table: person")

        return output

    @task
    def transform_data(input):
        import json
        import pandas as pd
        from utils import get_minio_client, write_data_to_minio

        minio_client = get_minio_client()

        item = minio_client.get_object("mybucket", input)

        json_data = json.loads(item.read().decode("utf-8"))
        json_data = json.loads(json_data)

        df = pd.DataFrame.from_dict(json_data, orient="index").T

        print(f"Got {len(df)} rows from PostgreSQL. Table: person")

        df = df.groupby("city").agg({"name": "count"}).reset_index().to_dict(orient="records")

        print(f"Got {len(df)} rows after aggregation. Table: person_count_by_city")

        write_data_to_minio(minio_client, df, f"{input}_groupped")

        return f"{input}_groupped"

    @task
    def consolidator(input):
        output = []

        for i in range(len(input)):
            output.append(input[i])

        return output

    @task
    def aggregate_data(input):
        import json
        from io import BytesIO
        import pandas as pd
        from utils import get_minio_client, write_data_to_minio

        minio_client = get_minio_client()

        print(input)

        dfs = []

        for city in input:

            local_data = minio_client.get_object("mybucket", city)

            json_data = json.load(BytesIO(local_data.read()))

            df = pd.DataFrame(json_data)

            dfs.append(df)

        df = pd.concat(dfs)

        print(f"Got {len(df)} rows from PostgreSQL. Table: person")

        output = df.to_json(date_format="iso", orient="records")

        write_data_to_minio(minio_client, output, "final")

        return "final"

    @task
    def load_data_to_ch(input):
        import json
        import pandas as pd
        from utils import get_minio_client, get_ch_client

        minio_client = get_minio_client()

        ch_client = get_ch_client()

        data = minio_client.get_object("mybucket", input)
        json_data = json.loads(data.read().decode("utf-8"))
        json_data = json.loads(json_data)
        df = pd.DataFrame(json_data)

        ch_client.insert_dataframe(
            "insert into person_count_by_city values", df, settings={"use_numpy": True}
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
