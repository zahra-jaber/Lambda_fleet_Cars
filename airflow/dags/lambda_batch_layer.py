"""DAG du batch layer : spark-submit sur HDFS -> batch_view, puis contrôle du watermark."""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PACKAGES = ("org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1")


def check_watermark():
    from cassandra.cluster import Cluster
    cluster = Cluster([os.getenv("CASSANDRA_HOST", "cassandra")])
    session = cluster.connect("fleet")
    row = session.execute("SELECT value FROM batch_meta WHERE key='watermark'").one()
    nb = session.execute("SELECT COUNT(*) FROM batch_view").one()[0]
    print(f"watermark={row.value if row else None}, batch_view rows={nb}")
    cluster.shutdown()
    if row is None:
        raise ValueError("Aucun watermark : le batch n'a rien écrit")


with DAG(
    dag_id="lambda_batch_layer",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",   # toutes les 15 min pour la démo ; "@hourly" ou "@daily" en prod
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["lambda", "batch"],
) as dag:
    batch_view = BashOperator(
        task_id="compute_batch_view",
        bash_command=(
            "spark-submit --master local[2] --conf spark.jars.ivy=/tmp/ivy "
            f"--packages {PACKAGES} /opt/jobs/batch_layer.py"
        ),
        env={"HDFS_URL": "hdfs://namenode:8020", "CASSANDRA_HOST": "cassandra",
             "HADOOP_USER_NAME": "root"},
        append_env=True,
    )
    verify = PythonOperator(task_id="verify_watermark", python_callable=check_watermark)
    batch_view >> verify
