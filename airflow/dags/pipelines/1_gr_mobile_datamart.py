import sys
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def build_gr_mobile_datamart(spark, logical_date, logger):
    from pyspark.sql import functions as F

    business_month_end = logical_date + relativedelta(months=1, days=-1)

    cp_mobile_daily_history_df = spark.table('gr_dm_sec.cp_mobile_daily_history')
    
    daily_features = (
        cp_mobile_daily_history_df
        .limit(1_000_000)
        .withColumn('tp_change_days',
                    F.datediff(F.to_date(F.lit(business_month_end)), F.to_date('tp_change_dt')))
        .withColumn('max_subs_fee_day_31d_days_ago',
                    F.datediff(F.to_date(F.lit(business_month_end)), F.to_date('max_subs_fee_dt_day_31d')))
        .withColumn('ep_tp_change_days',
                    F.datediff(F.to_date(F.lit(business_month_end)), F.to_date('ep_tp_change_dt')))
        .withColumn('device_first_imei_msisdn_days',
                    F.datediff(F.to_date(F.lit(business_month_end)), F.to_date('device_first_imei_msisdn_dt')))
        .withColumn('device_first_imei_msisdn_days',
                    F.datediff(F.to_date(F.lit(business_month_end)), F.to_date('device_first_imei_msisdn_dt')))
        .withColumn('last_change_smartphone_days',
                    F.datediff(F.to_date(F.lit(business_month_end)), F.to_date('last_change_smartphone_dt')))
        .withColumn('start_use_cur_smartp_mod_days',
                    F.datediff(F.to_date(F.lit(business_month_end)), F.to_date('start_use_cur_smartp_mod_dt')))
        .withColumn('mymts_max_activity_master_days',
                    F.datediff(F.to_date(F.lit(business_month_end)), F.to_date('mymts_max_activity_master_dt')))
        .withColumn('mymts_max_activity_slave_days',
                    F.datediff(F.to_date(F.lit(business_month_end)), F.to_date('mymts_max_activity_slave_dt')))
        .withColumn('mnp_portation_in_mts_days',
                    F.datediff(F.to_date(F.lit(business_month_end)), F.to_date('mnp_portation_in_mts_dt')))
        .withColumn('last_active_roam_day_90d_days_ago',
                    F.datediff(F.to_date(F.lit(business_month_end)), F.to_date('last_active_roam_dt_day_90d')))

        .limit(1_000_000)
    )

    res_df = (
        daily_features
        .join(spark.table("gr_dm.cp_mobile_monthly_history").drop('sappn', 'sappn_str', 'm_reg_cd', 'table_business_month'), ['app_n', 'regid'], 'left')
    )

    return res_df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("build_gr_datamart") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.cores.max", "3")\
        .config('spark.executor.memory', '7g')\
        .enableHiveSupport() \
        .getOrCreate()
    
    logical_date = datetime.now().date()
try:
    final_df = build_gr_mobile_datamart(spark, logical_date, logger)
    (
        final_df
        .repartition(4)
        .write
        .mode("overwrite")
        .format("orc")
        .saveAsTable("spfix_dm.mob_features_dm__1__base")
    )

finally:
    spark.stop()