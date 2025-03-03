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
