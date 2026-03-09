import sys
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def build_customer_sex_age_income(spark, logical_date, logger):
    from pyspark.sql import functions as F

    business_month_end = logical_date + relativedelta(months=1, days=-1)

    logger.info('Processing customer_sex ...')
    model_sex = (
        spark.table('ff_dm.scoring_customer_sex')
        .select('msisdn', F.col('model_sex'))
    )
    logger.info('Processing customer_age ...')
    model_age = (
        spark.table('ff_dm.scoring_customer_age')
        .select('msisdn', F.col('model_age'))
    )
    logger.info('Processing customer_income ...')
    income = (
        spark.table('gr_dm.dm_model_income__hist')
        .select('msisdn', F.col('model_income'))
    )
    logger.info('Joining tables ...')
    final_df = (
        model_sex
        .join(model_age, ['msisdn'], 'full')
        .join(income, ['msisdn'], 'full')
        .drop_duplicates(['msisdn'])
    )

    return final_df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("build_gr_sex_age") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.cores.max", "2")\
        .config('spark.executor.memory', '2g')\
        .enableHiveSupport() \
        .getOrCreate()
    
    logical_date = datetime.now().date()
try:
    final_df = build_customer_sex_age_income(spark, logical_date, logger)
    (
        final_df
        .repartition(1)
        .write
        .mode("overwrite")
        .format("orc")
        .saveAsTable("spfix_dm.mob_features_dm__2__base")
    )

finally:
    spark.stop()