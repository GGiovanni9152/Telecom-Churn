import sys
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def haversine(long_x, lat_x, long_y, lat_y):
    from pyspark.sql import functions as F

    return F.acos(
        F.sin(F.toRadians(lat_x)) * F.sin(F.toRadians(lat_y)) +
        F.cos(F.toRadians(lat_x)) * F.cos(F.toRadians(lat_y)) *
            F.cos(F.toRadians(long_x) - F.toRadians(long_y))
    ) * F.lit(6371.0 * 1000)

def mock_getZidCentroidUDF(col_name):
    from pyspark.sql import functions as F
    return F.struct(
        (F.rand() * 10 + 54).cast("double").alias("lat"),  
        (F.rand() * 20 + 30).cast("double").alias("lon")
    )

def build_moves_in_home_work_locations(spark, logical_date, logger):
    from pyspark.sql import functions as F

    logger.info('Building DataFrame with retro data of home/work locations...')
    part_dt_0 = datetime(logical_date.year, logical_date.month, 22).date()
    
    for month_ago in range(4):
        cur_part_dt = part_dt_0 - relativedelta(months=month_ago)
        cur_home_work = (
            spark.table('netscout_cdm.agg_subs_geo_home_work')
            .withColumn(f'home_lat_{month_ago}', F.col('home_place_coord.lat'))
            .withColumn(f'home_lon_{month_ago}', F.col('home_place_coord.lon'))
            .withColumn(f'work_lat_{month_ago}', F.col('work_place_coord.lat'))
            .withColumn(f'work_lon_{month_ago}', F.col('work_place_coord.lon'))
            .select(
                'msisdn',
                f'home_lat_{month_ago}', f'home_lon_{month_ago}',
                f'work_lat_{month_ago}', f'work_lon_{month_ago}'
            )
        )
        if month_ago == 0:
            final_home_work = cur_home_work.selectExpr('*')
        else:
            final_home_work = (
                final_home_work
                .join(cur_home_work, ['msisdn'], 'left')
            )

    logger.info('Building features from retro data...')
    final_home_work_features = (
        final_home_work
        .withColumn('home_diff_0_1', haversine('home_lon_0', 'home_lat_0', 'home_lon_1', 'home_lat_1'))
        .withColumn('work_diff_0_1', haversine('work_lon_0', 'work_lat_0', 'work_lon_1', 'work_lat_1'))
        .withColumn('home_diff_0_2', haversine('home_lon_0', 'home_lat_0', 'home_lon_2', 'home_lat_2'))
        .withColumn('work_diff_0_2', haversine('work_lon_0', 'work_lat_0', 'work_lon_2', 'work_lat_2'))
        .withColumn('home_diff_0_3', haversine('home_lon_0', 'home_lat_0', 'home_lon_3', 'home_lat_3'))
        .withColumn('work_diff_0_3', haversine('work_lon_0', 'work_lat_0', 'work_lon_3', 'work_lat_3'))

        .withColumn('home_diff_1_2', haversine('home_lon_1', 'home_lat_1', 'home_lon_2', 'home_lat_2'))
        .withColumn('work_diff_1_2', haversine('work_lon_1', 'work_lat_1', 'work_lon_2', 'work_lat_2'))
        .withColumn('home_diff_1_3', haversine('home_lon_1', 'home_lat_1', 'home_lon_3', 'home_lat_3'))
        .withColumn('work_diff_1_3', haversine('work_lon_1', 'work_lat_1', 'work_lon_3', 'work_lat_3'))

        .withColumn('home_diff_2_3', haversine('home_lon_2', 'home_lat_2', 'home_lon_3', 'home_lat_3'))
        .withColumn('work_diff_2_3', haversine('work_lon_2', 'work_lat_2', 'work_lon_3', 'work_lat_3'))

        .select(
            'msisdn',
            'home_diff_0_1', 'work_diff_0_1',
            'home_diff_0_2', 'work_diff_0_2',
            'home_diff_0_3', 'work_diff_0_3',
            'home_diff_1_2', 'work_diff_1_2',
            'home_diff_1_3', 'work_diff_1_3',
            'home_diff_2_3', 'work_diff_2_3'
        )

        .limit(2_000_000)
    )

    return final_home_work_features

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("build_move") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.cores.max", "4")\
        .config('spark.executor.memory', '4g')\
        .enableHiveSupport() \
        .getOrCreate()
    
    logical_date = datetime.now().date()
try:
    final_df = build_moves_in_home_work_locations(spark, logical_date, logger)
    (
        final_df
        .repartition(1)
        .write
        .mode("overwrite")
        .format("orc")
        .saveAsTable("spfix_dm.mob_features_dm__4__base")
    )

finally:
    spark.stop()