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
    task_id = 'start_mob_features',
    dag = dag
)

task_stage2 = DummyOperator(
    task_id = 'separate_feature_collection',
    dag = dag
)

task_stage3 = DummyOperator(
    task_id = 'build_prefinal',
    dag = dag
)

task_stage4 = DummyOperator(
    task_id = 'build_final_datamart',
    dag = dag
)

task_stage5 = DummyOperator(
    task_id = 'datamart_builded',
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
            --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
            --executor-memory 8g \
            --driver-memory 1g \
            /opt/airflow/dags/pipelines/1_gr_mobile_datamart.py {{ ds }}
    """,
    dag = dag
)

task_2_sex_age = BashOperator(
    task_id='build_sex_age',
    bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.sql.catalogImplementation=hive \
            --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse \
            --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
            --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
            /opt/airflow/dags/pipelines/2_sex_age_income.py {{ ds }}
    """,
    dag = dag
)

task_3_competitors = BashOperator(
    task_id='build_competitors',
    bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.sql.catalogImplementation=hive \
            --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse \
            --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
            --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
            /opt/airflow/dags/pipelines/3_competitors.py {{ ds }}
    """,
    dag = dag
)

task_4_move = BashOperator(
    task_id='build_move',
    bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.sql.catalogImplementation=hive \
            --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse \
            --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
            --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
            /opt/airflow/dags/pipelines/4_move.py {{ ds }}
    """,
    dag = dag
)

task_5_hosts = BashOperator(
    task_id='build_hosts',
    bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.sql.catalogImplementation=hive \
            --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse \
            --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
            --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
            /opt/airflow/dags/pipelines/5_hosts.py {{ ds }}
    """,
    dag = dag
)

task_6_final = BashOperator(
    task_id='build_final',
    bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.sql.catalogImplementation=hive \
            --conf spark.sql.warehouse.dir=hdfs://namenode:9000/user/hive/warehouse \
            --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
            --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
            /opt/airflow/dags/pipelines/6_final.py {{ ds }}
    """,
    dag = dag
)


# /opt/airflow/dags/pipelines/1_gr_mobile_datamart.py {{ ds }}
task_stage1 >> task_1_gr_datamart >> task_stage2

task_stage2 >> task_2_sex_age >> task_stage3
task_stage2 >> task_3_competitors >> task_stage3
task_stage2 >> task_4_move >> task_stage3

task_stage3 >> task_5_hosts >> task_stage4

task_stage4 >> task_6_final >> task_stage5
