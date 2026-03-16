import sys
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def build_competitors_contacts(spark, logical_date, logger):
    from pyspark.sql import functions as F

    business_month_start = datetime(logical_date.year, logical_date.month, 1).date()
    business_month_end = business_month_start + relativedelta(months=1, days= -1)

    logger.info('Processing sms part ...')
    sms_users = (
        spark.table("spfix_dm.join_to_cnum_sms")
        .withColumn('called_party_number', F.lower(F.regexp_replace(F.col('competitor'), '\.', '')))
        .groupby(['msisdn', 'called_party_number'])
        .agg(
            F.sum(F.col('call_sms')).alias('cnt_sms')
        )
    )

    logger.info('Processing calls part ...')

    calls_users = (
        spark.table("spfix_dm.join_to_cnum")
        .withColumnRenamed('phone_num', 'called_party_number')
        .withColumnRenamed('competitor', 'provider')
        .groupby(['msisdn', 'provider'])
        .agg(
            F.sum(F.col('call_cnt')).alias('cnt_calls')
        )
    )

    logger.info('Processing web part ...')
    web_users = (
        spark.table("spfix_dm.join_to_clickstream")
        .withColumnRenamed('host name', 'host')
        .withColumnRenamed('competitor', 'provider')
        
        .groupby(['msisdn', 'provider'])
        .agg(
            F.sum(F.col('request_cnt')).alias('cnt_req')
        )
    )

    final_df = (
        # join sms from competitors
        sms_users
        .withColumn('called_party_number', F.concat(F.lit('fix_comp_sms_cnt_'), F.col('called_party_number')))
        .groupBy('msisdn')
        .pivot('called_party_number')
        .agg(F.sum('cnt_sms'))
        
        .join(
            calls_users
            .withColumn('provider', F.lower(F.regexp_replace(F.col('provider'), '\.', '')))
            .withColumn('provider', F.concat(F.lit('fix_comp_calls_cnt_'), F.col('provider')))
            .groupBy('msisdn')
            .pivot('provider')
            .agg(F.sum('cnt_calls')),
            ['msisdn'], 'full'
        )
        
        .join(
            web_users
            .withColumn('provider', F.lower(F.regexp_replace(F.col('provider'), '\.', '')))
            .withColumn('provider', F.concat(F.lit('fix_comp_web_cnt_'), F.col('provider')))
            .groupBy('msisdn')
            .pivot('provider')
            .agg(F.sum('cnt_req')),
            ['msisdn'], 'full'
        )
        .fillna(0)
    )

    return final_df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("build_cpmpetitors") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.cores.max", "4")\
        .config('spark.executor.memory', '2g')\
        .enableHiveSupport() \
        .getOrCreate()
    
    logical_date = datetime.now().date()
try:
    final_df = build_competitors_contacts(spark, logical_date, logger)
    (
        final_df
        .repartition(1)
        .write
        .mode("overwrite")
        .format("orc")
        .saveAsTable("spfix_dm.mob_features_dm__3__base")
    )

finally:
    spark.stop()