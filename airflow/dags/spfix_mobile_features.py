import os
import sys

from datetime import datetime
from datetime import timedelta

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator 
from airflow.operators.bash_operator import BashOperator
from airflow import DAG

dag = DAG(
    'mobile_features',
    description='Build mobile features datamart',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False
)

task_stage1 = DummyOperator(
    task_id = 'separate_feature_collection',
    dag = dag
)

task_stage2 = DummyOperator(
    task_id = 'build_final_datamart',
    dag = dag
)

task_1_gr_datamart = BashOperator(
    task_id='build_gr_datamart',
    bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.sql.catalogImplementation=hive \
            --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse \
            --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
            --conf spark.hadoop.hadoop.security.authentication=simple \
            --conf spark.hadoop.hadoop.security.authorization=false \
            --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
            --conf spark.yarn.appMasterEnv.HADOOP_USER_NAME=hive \
            --conf spark.executorEnv.HADOOP_USER_NAME=hive\
            --executor-memory 5g \
            --driver-memory 1g \
            /opt/airflow/dags/pipelines/2_sex_age_income.py {{ ds }}
    """,
    dag = dag
)
# /opt/airflow/dags/pipelines/1_gr_mobile_datamart.py {{ ds }}
task_stage1 >> task_1_gr_datamart >> task_stage2