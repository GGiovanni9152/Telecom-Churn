import sys
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def build_final_mobile_features(spark, logical_date, logger):
    """Скрипт сборки абонентской базы для spfix_feature_store"""

    import pyspark.sql.functions as F

    logical_date = datetime.strptime(logical_date, '%Y-%m-%d').date()
    business_month = str(datetime(logical_date.year, logical_date.month, 1).date())

    logger.info('Building final dataframe...')
    logger.info(f'Logical date: {logical_date}')
    logger.info(f'Reporting date: {str(business_month)}')

    logger.info('Collecting all sources...')
    final_df = (
        spark.table("spfix_dm.mob_features_dm__1__base")
        .limit(700_000)
        .fillna(-1)
        .join(
            spark.table('spfix_dm.mob_features_dm__2__base'),
            ['msisdn'], 'left'
        )
        .fillna('unk')
        .join(
            spark.table('spfix_dm.mob_features_dm__3__base'),
            ['msisdn'], 'left'
        )
        .fillna(0)
        .join(
            spark.table('spfix_dm.mob_features_dm__4__base'),
            ['msisdn'], 'left'
        )
        .fillna(0)
        .join(
            spark.table('spfix_dm.mob_features_dm__5__base')
            .drop('app_n', 'regid'),
            ['msisdn'], 'left'
        )
        .fillna(0)
        .withColumn('build_dt', F.current_date())
        .withColumn('table_business_month', F.lit(business_month))
    )

    return final_df, business_month

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
    final_df, dt = build_final_mobile_features(spark, logical_date, logger)
    (
        final_df
        .repartition(8)
        .write
        .mode("overwrite")
        .format("orc")
        .saveAsTable("spfix_dm.mob_features")
    )

finally:
    spark.stop()