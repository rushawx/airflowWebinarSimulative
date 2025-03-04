def get_minio_client():
    import json
    from airflow.hooks.base import BaseHook
    from minio import Minio

    minio_conn = BaseHook.get_connection("minio")

    endpoint_url = json.loads(minio_conn.extra)["endpoint_url"]

    minio_client = Minio(
        endpoint_url,
        access_key=minio_conn.login,
        secret_key=minio_conn.password,
        secure=False,
    )

    return minio_client


def write_data_to_minio(minio_client, data, name):
    import json
    from io import BytesIO

    data_json = json.dumps(data).encode("utf-8")
    data_stream = BytesIO(data_json)
    minio_client.put_object(
        "mybucket",
        name,
        data_stream,
        len(data_json),
        content_type="application/json",
    )


def get_pg_engine():
    import sqlalchemy
    from airflow.hooks.base import BaseHook

    pg_conn = BaseHook.get_connection("postgres")

    dsn = f"postgresql://{pg_conn.login}:{pg_conn.password}@{pg_conn.host}"
    dsn += f":{pg_conn.port}/{pg_conn.schema}"

    pg_engine = sqlalchemy.create_engine(dsn)

    return pg_engine


def get_ch_client():
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

    return client
