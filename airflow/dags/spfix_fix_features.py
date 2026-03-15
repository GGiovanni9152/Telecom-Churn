import os
import sys

from datetime import datetime
from datetime import timedelta

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator 
from airflow.operators.bash_operator import BashOperator
from airflow import DAG

dag = DAG(
    'fix_features',
    description='Build mobifixle features datamart',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False
)

task_stage1 = DummyOperator(
    task_id = 'start_fix_features',
    dag = dag
)

task_stage2 = DummyOperator(
    task_id = 'fix_features_builded',
    dag = dag
)

task_fix_features = BashOperator(
    task_id='build_features',
    bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.sql.catalogImplementation=hive \
            --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse \
            --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
            --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
            --executor-memory 6g \
            --driver-memory 1g \
            /opt/airflow/dags/pipelines/fix_features.py {{ ds }}
    """,
    dag = dag
)

task_stage1 >> task_fix_features >> task_stage2
