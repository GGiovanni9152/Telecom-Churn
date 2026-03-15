import sys
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession

from datetime import datetime
from datetime import timedelta

import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pyspark.sql.types as T

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fix_conv(spark, business_month_end):
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    red_convergent = spark.table('mtsfix_dm.agg_mtsfix_red_convergent_fixa_a')
    mgts_convergent = spark.table('mtsfix_dm.agg_mtsfix_red_convergent_mgts_fixa_a')

    all_convergent_base = (
        red_convergent
        .unionAll(mgts_convergent)
        .where(F.col('dt_from') <= business_month_end)
        .where(F.coalesce(F.col('dt_to'), F.lit('9999-12-31')) > business_month_end)
        .select(
                'foris_id', 
                'foris_acc_id', 
                'foris_msisdn', 
                'fix_acc_num', 
                'fix_acc_id', 
                'personal_account_number', 
                'dt_from', 
                'foris_tariff_plan', 
                'fix_billing_id'
        )
    )

    foris_identity = (
        all_convergent_base
        .select(
                'foris_id', 
                'foris_acc_id', 
                'personal_account_number', 
                F.col('foris_id').alias('billing_id'),
                F.col('foris_acc_id').alias('acc_id'),
                F.col('personal_account_number').alias('acc_num'),
                'foris_msisdn', 
                'foris_tariff_plan', 
                F.lit(1).alias('is_foris')
        )
        .drop_duplicates()
    )

    fix_identity = (
        all_convergent_base
        .select(
                'foris_id', 
                'foris_acc_id', 
                'personal_account_number', 
                F.col('fix_billing_id').alias('billing_id'),
                F.col('fix_acc_id').alias('acc_id'),
                F.col('fix_acc_num').alias('acc_num'),
                'foris_msisdn', 
                'foris_tariff_plan', 
                F.lit(0).alias('is_foris')
        )
        .drop_duplicates()
    )

    foris_and_fix_identities = (foris_identity.unionAll(fix_identity))

    acc_con = spark.table('mtsfix_dm.agg_mtsfix_acc_con_fixa_a')

    adding_acc_con = (
        foris_and_fix_identities
        .join(
            acc_con
            .withColumn(
                'row_num',
                F.row_number().over(Window.partitionBy('billing_id', 'acc_id').orderBy(F.desc('dt_from')))
            )
            .where(F.col('row_num') == 1)
            .drop('row_num')
            .select(
                'acc_id',
                'bopos_id',
                'contract_id',
                'contract_num',
                'siebel_contract_id',
                'customer_id',
                'city_id',
                'reg_id',
                'mr_id',
                'customer_type_id',
                'calculation_method_id',
                'dt_from',
                'is_noncommercial',
                'is_closed',
                'parent_contract_id',
                'marketing_category_id',
                'siebel_status_id',
                'service_provider_id',
                'is_b2b',
                'orders_credit',
                'credit',
                'is_archive',
                'customer_category_id',
                'billing_id'
            ),
            ['billing_id', 'acc_id'],
            'left'
        )
    )
    return adding_acc_con

def fix_add_att(spark, business_month_end):
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    acc_att = spark.table('mtsfix_dm.agg_mtsfix_acc_att_fixa_a')

    acc_att_processed = (
        acc_att
        .select(
            'billing_id',
            'acc_id',
            'dt_open',
            'dt_close',
            'dt_open_spd',
            'dt_close_spd',
            'dt_open_ctv',
            'dt_close_ctv',
            'dt_open_atv',
            'dt_close_atv',
            'dt_open_tlf',
            'dt_close_tlf',
            'dt_open_ictv',
            'dt_close_ictv'
        )
        .withColumn(
            'dt_open',
            F.when(
                (F.col('dt_open') < '2000-01-01') | (F.col('dt_open').isNull()),
                F.least(
                    F.to_date(F.col('dt_open_spd')), F.to_date(F.col('dt_open_ctv')), 
                    F.to_date(F.col('dt_open_atv')), 
                    F.to_date(F.col('dt_open_tlf')), F.to_date(F.col('dt_open_ictv'))
                )
            ).otherwise(F.to_date(F.col('dt_open')))
        )
        .where(F.col('dt_open').isNotNull())
        .withColumn('is_acc_att', F.lit(1))
        .withColumn(
            'is_contract_closed_flg', 
            F.when(F.to_date(F.col('dt_close')) <= F.lit(business_month_end), F.lit(1)).otherwise(F.lit(0))           
        )
        
        .withColumn(
            'live_dur_contract',
            F.when((F.col('dt_close') <= F.lit(business_month_end)) & F.col('dt_close').isNotNull(),
                F.lit(F.datediff(F.to_date(F.col('dt_close')), F.col('dt_open')))      
            ).otherwise(F.lit(F.datediff(F.lit(business_month_end), F.col('dt_open'))))
        )

        .withColumn(
            'live_dur_fix_spd',
            F.when((F.col('dt_close_spd') <= F.lit(business_month_end)) & F.col('dt_close_spd').isNotNull(),
                F.lit(F.datediff(F.to_date(F.col('dt_close_spd')), F.col('dt_open_spd')))      
            ).otherwise(F.lit(F.datediff(F.lit(business_month_end), F.col('dt_open_spd'))))
        )

        .withColumn(
            'live_dur_fix_telef',
            F.when((F.col('dt_close_tlf') <= F.lit(business_month_end)) & F.col('dt_close_tlf').isNotNull(),
                F.lit(F.datediff(F.to_date(F.col('dt_close_tlf')), F.col('dt_open_tlf')))      
            ).otherwise(F.lit(F.datediff(F.lit(business_month_end), F.col('dt_open_tlf'))))
        )

        .withColumn(
            'live_dur_fix_atv',
            F.when((F.col('dt_close_atv') <= F.lit(business_month_end)) & F.col('dt_close_atv').isNotNull(),
                F.lit(F.datediff(F.to_date(F.col('dt_close_atv')), F.col('dt_open_atv')))      
            ).otherwise(F.lit(F.datediff(F.lit(business_month_end), F.col('dt_open_atv'))))
        )

        .withColumn(
            'live_dur_fix_ctv',
            F.when((F.col('dt_close_ctv') <= F.lit(business_month_end)) & F.col('dt_close_ctv').isNotNull(),
                F.lit(F.datediff(F.to_date(F.col('dt_close_ctv')), F.col('dt_open_ctv')))      
            ).otherwise(F.lit(F.datediff(F.lit(business_month_end), F.col('dt_open_ctv'))))
        )

        .withColumn(
            'live_dur_fix_ictv',
            F.when((F.col('dt_close_ictv') <= F.lit(business_month_end)) & F.col('dt_close_ictv').isNotNull(),
                F.lit(F.datediff(F.to_date(F.col('dt_close_ictv')), F.col('dt_open_ictv')))      
            ).otherwise(F.lit(F.datediff(F.lit(business_month_end), F.col('dt_open_ictv'))))
        )

        .withColumn(
            'has_opened_spd',
            F.when(((F.col('dt_close_spd') > F.lit(business_month_end))
                   | (~F.col('dt_close_spd').isNotNull()))
                  & (F.col('dt_open_spd') <= F.lit(business_month_end)), 1).otherwise(0)
        )

        .withColumn(
            'has_opened_ctv',
            F.when(((F.col('dt_close_ctv') > F.lit(business_month_end))
                   | (~F.col('dt_close_ctv').isNotNull()))
                  & (F.col('dt_open_ctv') <= F.lit(business_month_end)), 1).otherwise(0)
        )

        .withColumn(
            'has_opened_atv',
            F.when(((F.col('dt_close_atv') > F.lit(business_month_end))
                   | (~F.col('dt_close_atv').isNotNull()))
                  & (F.col('dt_open_atv') <= F.lit(business_month_end)), 1).otherwise(0)
        )

        .withColumn(
            'has_opened_tlf',
            F.when(((F.col('dt_close_tlf') > F.lit(business_month_end))
                   | (~F.col('dt_close_tlf').isNotNull()))
                  & (F.col('dt_open_tlf') <= F.lit(business_month_end)), 1).otherwise(0)
        )

        .withColumn(
            'has_opened_ictv',
            F.when(((F.col('dt_close_ictv') > F.lit(business_month_end))
                   | (~F.col('dt_close_ictv').isNotNull()))
                  & (F.col('dt_open_ictv') <= F.lit(business_month_end)), 1).otherwise(0)
        )

        .drop(
            'dt_close',
            'dt_close_spd',
            'dt_close_ctv',
            'dt_close_atv',
            'dt_close_tlf',
            'dt_close_ictv'
        )
    )

    return acc_att_processed

def fix_add_pay(spark, business_month_end):
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    dt_begin_fix_pay = pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(5)

    m0 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(0)).date()
    m1 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(1)).date()
    m2 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(2)).date()
    m3 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(3)).date()
    m4 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(4)).date()

    print(f'dt_begin_fix_pay: {dt_begin_fix_pay}')
    print(f'm0: {m0}')
    print(f'm1: {m1}')
    print(f'm2: {m2}')
    print(f'm3: {m3}')
    print(f'm4: {m4}')

    fix_pay = spark.table('mtsfix_dm.agg_mtsfix_pay_fixa_a')

    fix_pay_processed = (
        fix_pay
        .withColumnRenamed('dt', 'pay_date')
        
        .where(F.col('pay_date') > F.lit(dt_begin_fix_pay))
        .where(F.col('pay_date') <= F.lit(m0))
        .withColumn('dt', F.last_day(F.to_date('pay_date')))
        .select('billing_id', 'acc_id', 'dt', 'pay_class', 'amount')
    )

    pay_class = (
        fix_pay_processed
        .withColumn(
            'sum_class_mm',
            F.sum('amount').over(Window.partitionBy('billing_id', 'acc_id', 'dt', 'pay_class'))
        )

        .withColumn(
            'rnk',
            F.row_number().over(
                Window.partitionBy('billing_id', 'acc_id', 'dt').orderBy(F.col('sum_class_mm').desc()))
        )
        .where(F.col('rnk') == 1)
        .withColumn(
            'pay_class_00mm',
            F.when(F.col('dt') == F.lit(m0), F.col('pay_class')).otherwise(F.lit(0))
        )

        .withColumn(
            'pay_class_01mm',
            F.when(F.col('dt') == F.lit(m1), F.col('pay_class')).otherwise(F.lit(0))
        )

        .withColumn(
            'pay_class_02mm',
            F.when(F.col('dt') == F.lit(m2), F.col('pay_class')).otherwise(F.lit(0))
        )

        .withColumn(
            'pay_class_03mm',
            F.when(F.col('dt') == F.lit(m3), F.col('pay_class')).otherwise(F.lit(0))
        )

        .withColumn(
            'pay_class_04mm',
            F.when(F.col('dt') == F.lit(m4), F.col('pay_class')).otherwise(F.lit(0))
        )

        .groupBy('billing_id', 'acc_id')
        .agg(
            F.max('pay_class_00mm').alias('pay_class_00mm'),
            F.max('pay_class_01mm').alias('pay_class_01mm'),
            F.max('pay_class_02mm').alias('pay_class_02mm'),
            F.max('pay_class_03mm').alias('pay_class_03mm'),
            F.max('pay_class_04mm').alias('pay_class_04mm')
        )
    )

    pay_month = (
        fix_pay_processed
        .withColumn('pay_00mm', F.when(F.col('dt') == F.lit(m0), F.col('amount')).otherwise(F.lit(0)))
        .withColumn('pay_01mm', F.when(F.col('dt') == F.lit(m1), F.col('amount')).otherwise(F.lit(0)))
        .withColumn('pay_02mm', F.when(F.col('dt') == F.lit(m2), F.col('amount')).otherwise(F.lit(0)))
        .withColumn('pay_03mm', F.when(F.col('dt') == F.lit(m3), F.col('amount')).otherwise(F.lit(0)))
        .withColumn('pay_04mm', F.when(F.col('dt') == F.lit(m4), F.col('amount')).otherwise(F.lit(0)))

        .groupBy('billing_id', 'acc_id')
        .agg(
            F.sum('pay_00mm').alias('pay_00mm'),
            F.sum('pay_01mm').alias('pay_01mm'),
            F.sum('pay_02mm').alias('pay_02mm'),
            F.sum('pay_03mm').alias('pay_03mm'),
            F.sum('pay_04mm').alias('pay_04mm')
        )
        .withColumn('is_pay', F.lit(1))
    )

    result = (pay_month.join(pay_class, ['billing_id', 'acc_id'], 'left'))

    return result

def fix_add_traffic(spark, business_month_end):
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    dt_begin_fix_traffic = pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(5)

    m0 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(0)).date()
    m1 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(1)).date()
    m2 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(2)).date()
    m3 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(3)).date()
    m4 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(4)).date()

    print(f'dt_begin_fix_traffic: {dt_begin_fix_traffic}')
    print(f'm0: {m0}')
    print(f'm1: {m1}')
    print(f'm2: {m2}')
    print(f'm3: {m3}')
    print(f'm4: {m4}')

    fix_clc = spark.table('mtsfix_dm_sec.agg_mtsfix_clc_fixa_a')

    fix_trafic = (
        fix_clc
        .where((F.col('dt') > dt_begin_fix_traffic) & (F.col('dt') <= business_month_end)
        )

        .withColumn('dt', F.col('dt').cast('date'))
        .withColumn(
            'gb',
            F.round(
                F.when(F.col('metric_unit_number') == 2,
                       F.col('number_of_units') / 8 / 1024 / 1024 / 1024)   
                      .when(F.col('metric_unit_number') == 3,
                           F.col('number_of_units') / 1024 / 1024 / 1024)
                      .when(F.col('metric_unit_number') == 4,
                           F.col('number_of_units') / 1024 / 1024)
                      .when(F.col('metric_unit_number') == 5,
                           F.col('number_of_units') / 1024).otherwise(F.lit(0)), 2
                )
        )
        .groupBy('billing_id', 'acc_id')
        .agg(
            F.sum(
                F.when(F.col('dt') == F.lit(m0), F.col('gb')).otherwise(F.lit(0))
            ).alias('shpd_gb_0'),
            F.sum(
                F.when(F.col('dt') == F.lit(m1), F.col('gb')).otherwise(F.lit(0))
            ).alias('shpd_gb_1'),
            F.sum(
                F.when(F.col('dt') == F.lit(m2), F.col('gb')).otherwise(F.lit(0))
            ).alias('shpd_gb_2'),
            F.sum(
                F.when(F.col('dt') == F.lit(m3), F.col('gb')).otherwise(F.lit(0))
            ).alias('shpd_gb_3'),
            F.sum(
                F.when(F.col('dt') == F.lit(m4), F.col('gb')).otherwise(F.lit(0))
            ).alias('shpd_gb_4')
        )
        .withColumn('is_traffic', F.lit(1))
    )

    return fix_trafic

def fix_add_equip(spark, business_month_end):
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window
    import itertools

    equipment_types = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
    provide_types = [1, 2, 3, 4]
    categs = list(itertools.product(equipment_types, provide_types))

    names_categ = [f'equipment_type_{str(cat[0])}_provide_type_{str(cat[1])}' for cat in categs]

    exprs = [
        F.when(
            (F.col('equipment_type') == cat[0]) & (F.col('provide_type') == cat[1]), 1
        ).otherwise(0).alias(f'equipment_type_{str(cat[0])}_provide_type_{str(cat[1])}') for cat in categs
    ]

    fix_eqipment = spark.table('mtsfix_dm.agg_mtsfix_equipment_fixa_a')

    fix_eqipment_processed = (
        fix_eqipment
        .where(F.col('dt_from') <= F.lit(business_month_end))
        .where(F.coalesce(F.col('dt_to'), F.lit('9999-12-31')) > F.lit(business_month_end))
        .select(exprs + ['acc_id', 'billing_id', 'equipment_type', 'provide_type'])
        .groupBy('billing_id', 'acc_id')
        .max(*names_categ)
        .withColumn('is_equip', F.lit(1))
    )

    fix_old_eq = (
        fix_eqipment
        .select('billing_id', 'acc_id')
        .distinct()
        .withColumn('had_old_eq', F.lit(1))
    )

    fix_block_eq = (
        fix_eqipment
        .where(F.col('is_blocked') == 1)
        .select('billing_id', 'acc_id')
        .distinct()
        .withColumn('had_block_eq', F.lit(1))
    )

    for column in fix_eqipment_processed.drop('billing_id', 'acc_id', 'is_equip').columns:
        start_index = column.find('(')
        end_index = column.find(')')

        if start_index and end_index:
            fix_eqipment_processed = (
                fix_eqipment_processed
                .withColumnRenamed(column, column[start_index + 1: end_index])
            )
    result = (
        fix_eqipment_processed
        .join(fix_old_eq, ['billing_id', 'acc_id'], 'left')
        .join(fix_block_eq, ['billing_id', 'acc_id'], 'left')
    )

    return result

def fix_add_remedy(spark, business_month_end, adding_fix_equipment):
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    dt_begin_remedytb = pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(5)

    m0 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(0)).date()
    m1 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(1)).date()
    m2 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(2)).date()
    m3 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(3)).date()
    m4 = (pd.to_datetime(business_month_end) - pd.offsets.MonthEnd(4)).date()

    print(f'dt_begin_fix_remedy: {dt_begin_remedytb}')
    print(f'm0: {m0}')
    print(f'm1: {m1}')
    print(f'm2: {m2}')
    print(f'm3: {m3}')
    print(f'm4: {m4}')

    remedytb_processed = (
        spark.table('raw.mtsru_remedytb_msk__aradmin_stb_singlinc_dashb_all')
        .where(F.col('raw_dt') > F.lit(dt_begin_remedytb))
        .where(F.col('dogtype') == 'Фиксированный')
        .where(F.col('createdate_dt_orig') > F.lit(dt_begin_remedytb))
        .where(F.col('createdate_dt_orig') <= F.lit(business_month_end))
        .where(F.col('request_id').isNotNull())
        .select(
            "request_id",
            "status",
            "priority",
            "contract_num",
            "closetime_dt_orig",
            "createdate_dt_orig"
        )
        .drop_duplicates()
    )

    remedytb_processed_grouped_request = (
        remedytb_processed
        .groupBy('request_id')
        .agg(
            F.count_distinct('contract_num').alias('cnt_con')
        )
        .where(F.col('cnt_con') == 1)
    )

    remedytb_agg = (
        remedytb_processed
        .join(remedytb_processed_grouped_request, ['request_id'], 'inner')
        .select(
            'request_id',
            'status',
            'priority',
            'contract_num',
            'closetime_dt_orig',
            'createdate_dt_orig',
            F.last_day(F.to_date(F.col('createdate_dt_orig'))).alias('create_dt')
        )
    )

    prefinal_remedytb_agg = (
        adding_fix_equipment
        .select('billing_id', 'acc_id', 'acc_num', 'contract_num')
        .drop_duplicates()
        .join(remedytb_agg, ['contract_num'], 'inner')
        .withColumn(
            'complaints_num_active_expec',
            F.when(F.col('status') == 'Активное ожидание', 1).otherwise(0)
        )
        .withColumn('complaints_num_active', F.when(F.col('status') == 'В работе', 1).otherwise(0))
        .withColumn('complaints_num_close', F.when(F.col('status') == 'Закрыт', 1).otherwise(0))
        
        .withColumn(
            'complaints_num_priority_low',
            F.when(F.col('priority') == 'Низкий', 1).otherwise(0)
        )
        
        .withColumn(
            'complaints_num_priority_mid',
            F.when(F.col('priority') == 'Средний', 1).otherwise(0)
        )

        .withColumn(
            'complaints_num_priority_lowmid',
            F.when(F.col('priority') == 'Ниже среднего', 1).otherwise(0)
        )

        .withColumn(
            'complaints_num_priority_high',
            F.when(F.col('priority') == 'Высокий', 1).otherwise(0)
        )

        .withColumn(
            'complaints_num_priority_highmid',
            F.when(F.col('priority') == 'Выше среднего', 1).otherwise(0)
        )

        .withColumn(
            'complaints_num_priority_highest',
            F.when(F.col('priority') == 'Наивысший', 1).otherwise(0)
        )

        .withColumn(
            'zhalobi_lifetime_hours',
            F.when(F.col('status').isin(['Закрыт', 'Решен']),
                    (F.col('closetime_dt_orig').cast('long') - F.col('createdate_dt_orig').cast('long')) / 3600
                  ).otherwise(0)
            
        )

        .withColumn(
            'cnt_zhalobi_00mm',
            F.when(F.col('create_dt') == F.lit(m0), 1).otherwise(F.lit(0))
        )
        .withColumn(
            'cnt_zhalobi_01mm',
            F.when(F.col('create_dt') == F.lit(m1), 1).otherwise(F.lit(0))
        )
        .withColumn(
            'cnt_zhalobi_02mm',
            F.when(F.col('create_dt') == F.lit(m2), 1).otherwise(F.lit(0))
        )
        .withColumn(
            'cnt_zhalobi_03mm',
            F.when(F.col('create_dt') == F.lit(m3), 1).otherwise(F.lit(0))
        )
        .withColumn(
            'cnt_zhalobi_04mm',
            F.when(F.col('create_dt') == F.lit(m4), 1).otherwise(F.lit(0))
        )

        .groupBy('billing_id', 'acc_id', 'acc_num', 'contract_num', 'request_id')
        .agg(
            F.max('complaints_num_active_expec').alias('complaints_num_active_expec'),
            F.max('complaints_num_active').alias('complaints_num_active'),
            F.max('complaints_num_close').alias('complaints_num_close'),
            F.max('complaints_num_priority_low').alias('complaints_num_priority_low'),
            F.max('complaints_num_priority_mid').alias('complaints_num_priority_mid'),
            F.max('complaints_num_priority_lowmid').alias('complaints_num_priority_lowmid'),
            F.max('complaints_num_priority_high').alias('complaints_num_priority_high'),
            F.max('complaints_num_priority_highmid').alias('complaints_num_priority_highmid'),
            F.max('complaints_num_priority_highest').alias('complaints_num_priority_highest'),
            F.max('zhalobi_lifetime_hours').alias('zhalobi_lifetime_hours'),
            F.max('cnt_zhalobi_00mm').alias('cnt_zhalobi_00mm'),
            F.max('cnt_zhalobi_01mm').alias('cnt_zhalobi_01mm'),
            F.max('cnt_zhalobi_02mm').alias('cnt_zhalobi_02mm'),
            F.max('cnt_zhalobi_03mm').alias('cnt_zhalobi_03mm'),
            F.max('cnt_zhalobi_04mm').alias('cnt_zhalobi_04mm')
        )
    )

    final_remedytb_agg = (
        prefinal_remedytb_agg
        .groupBy('billing_id', 'acc_id', 'acc_num', 'contract_num')
        .agg(
            F.count_distinct('request_id').alias('cnt_zhalobi'),
            
            F.sum('cnt_zhalobi_00mm').alias('cnt_zhalobi_00mm'),
            F.sum('cnt_zhalobi_01mm').alias('cnt_zhalobi_01mm'),
            F.sum('cnt_zhalobi_02mm').alias('cnt_zhalobi_02mm'),
            F.sum('cnt_zhalobi_03mm').alias('cnt_zhalobi_03mm'),
            F.sum('cnt_zhalobi_04mm').alias('cnt_zhalobi_04mm'),
            
            F.min('zhalobi_lifetime_hours').alias('min_zhalobi_lifetime_hours'),
            F.max('zhalobi_lifetime_hours').alias('max_zhalobi_lifetime_hours'),
            
            F.sum('complaints_num_active_expec').alias('complaints_num_active_expec'),
            F.sum('complaints_num_active').alias('complaints_num_active'),
            F.sum('complaints_num_close').alias('complaints_num_close'),
            F.sum('complaints_num_priority_low').alias('complaints_num_priority_low'),
            F.sum('complaints_num_priority_mid').alias('complaints_num_priority_mid'),
            F.sum('complaints_num_priority_lowmid').alias('complaints_num_priority_lowmid'),
            F.sum('complaints_num_priority_high').alias('complaints_num_priority_high'),
            F.sum('complaints_num_priority_highmid').alias('complaints_num_priority_highmid'),
            F.sum('complaints_num_priority_highest').alias('complaints_num_priority_highest')
        )
        .withColumn('is_zhalobi', F.lit(1))
    )

    return final_remedytb_agg

def fix_add_serv(spark, business_month_end):
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    fix_services = spark.table('mtsfix_dm.agg_mtsfix_app_service_fixa_a')
    
    services = (
        fix_services
        .where(F.col('dt_from') <= F.current_date())
        .where(F.col('dt_to') > F.current_date())
        .where(F.col('is_closed') == 0)
    )

    count_closed = (
        fix_services
        .select('billing_id', 'acc_id', 'app_service_id', 'is_closed')
        .distinct()
        .groupBy('billing_id', 'acc_id')
        .agg(
            F.sum('is_closed').alias('closed_cnt')
        )
    )

    count_basic = (
        services
        .select('billing_id', 'acc_id', 'app_service_id', 'is_b2c_basic_service')
        .distinct()
        .groupBy('billing_id', 'acc_id')
        .agg(
            F.count('is_b2c_basic_service').alias('all_serv_cnt'),
            F.sum('is_b2c_basic_service').alias('basic_serv_cnt')
        )
        .withColumn('not_basic_serv_cnt', F.col('all_serv_cnt') - F.col('basic_serv_cnt'))
        .drop('all_serv_cnt')
    )

    detail_serv = (
        services
        .groupBy('billing_id', 'acc_id')
        .agg(
            F.count_distinct('app_service_id').alias('active_serv_cnt')
        )
        
        .join(count_closed, ['billing_id', 'acc_id'], 'left')
        .join(count_basic, ['billing_id', 'acc_id'], 'left')
    )

    disc = spark.table('mtsfix_dm_sec.agg_mtsfix_discount_fixa_a')

    disc_processed = (
        disc
        .where(F.col('dt_from') <= F.current_date())
        .where(F.col('dt_to') > F.current_date())
    )

    acc_disc = (
        services
        .join(disc_processed, ['billing_id', 'acc_id', 'app_service_id'], 'left')

        .groupBy('billing_id', 'acc_id')
        .agg(
            F.sum('discount_price').alias('all_disc_sum')
        )
    )

    result = (
        detail_serv
        .join(acc_disc, ['billing_id', 'acc_id'], 'left')
    )

    return result

def fix_add_gr(spark):
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    gr_features = spark.table('gr_dm.ma_fix_acc_services_history')

    gr_features_fix = (
        gr_features
        .select(
            'billing_id',
            'customer_id',
            'version_dt',
            'bb_rent_amount',
            'tv_rent_amount',
            'tlf_rent_amount',
            'tv_line_coast',
            'tv_mult_coast',
            'gender',
            'customer_age',
            'sale_channel',
            'lk_use_count',
            'lk_use_count_3m',
            'all_sr_count_30d',
            'complaint_sr_count_30d',
            'cm_sr_count_30d',
            'cm_sr_count_180d'
        )

        .withColumn(
            'row_num',
            F.row_number().over(Window.partitionBy('billing_id', 'customer_id').orderBy(F.desc('version_dt')))
        )
        .where(F.col('row_num') == 1)
        .drop('version_dt', 'row_num')
        .distinct()
    )

    return gr_features_fix

def fix_final_df(union_df, business_month_end):
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    final_df = (
        union_df
        .withColumn('billing_id', F.when(F.col('is_foris') == 0, F.col('billing_id')).otherwise(F.lit(None)))
        .withColumn('acc_id', F.when(F.col('is_foris') == 0, F.col('acc_id')).otherwise(F.lit(None)))
        .withColumn('acc_num', F.when(F.col('is_foris') == 0, F.col('acc_num')).otherwise(F.lit(None)))
        .withColumn(
            'foris_contract_num', 
            F.when(F.col('is_foris') == 1, F.col('contract_num')).otherwise(F.lit(None))
        )
        .withColumn(
            'total_live_dur', F.lit(F.datediff(F.to_date(F.lit(business_month_end)), F.col('dt_from')))
        )
        .groupBy('foris_id', 'foris_acc_id', 'personal_account_number', 'foris_msisdn', 'foris_tariff_plan')
        .agg(
            F.first('billing_id', ignorenulls = True).alias('billing_id'),
            F.first('acc_id', ignorenulls = True).alias('acc_id'),
            F.first('acc_num', ignorenulls = True).alias('acc_num'),
            F.first('foris_contract_num', ignorenulls = True).alias('foris_contract_num'),
            
            F.max('total_live_dur').alias('bopos_id'),
            F.first('is_noncommercial', ignorenulls = True).alias('is_noncommercial'),
            F.max('is_closed').alias('is_closed'),
            F.sum('orders_credit').alias('orders_credit'),
            F.sum('credit').alias('credit'),
            F.max('is_archive').alias('is_archive'),
            F.max('is_contract_closed_flg').alias('is_contract_closed_flg'),
            
            F.max('live_dur_contract').alias('live_dur_contract'),
            F.max('live_dur_fix_spd').alias('live_dur_fix_spd'),
            F.max('live_dur_fix_telef').alias('live_dur_fix_telef'),
            F.max('live_dur_fix_atv').alias('live_dur_fix_atv'),
            F.max('live_dur_fix_ctv').alias('live_dur_fix_ctv'),
            F.max('live_dur_fix_ictv').alias('live_dur_fix_ictv'),
            
            F.max('has_opened_spd').alias('has_opened_spd'),
            F.max('has_opened_ctv').alias('has_opened_ctv'),
            F.max('has_opened_atv').alias('has_opened_atv'),
            F.max('has_opened_tlf').alias('has_opened_tlf'),
            F.max('has_opened_ictv').alias('has_opened_ictv'),

            F.sum('pay_00mm').alias('pay_00mm'),
            F.sum('pay_01mm').alias('pay_01mm'),
            F.sum('pay_02mm').alias('pay_02mm'),
            F.sum('pay_03mm').alias('pay_03mm'),
            F.sum('pay_04mm').alias('pay_04mm'),

            F.sum('shpd_gb_0').alias('shpd_gb_0'),
            F.sum('shpd_gb_1').alias('shpd_gb_1'),
            F.sum('shpd_gb_2').alias('shpd_gb_2'),
            F.sum('shpd_gb_3').alias('shpd_gb_3'),
            F.sum('shpd_gb_4').alias('shpd_gb_4'),

            F.sum('cnt_zhalobi').alias('cnt_zhalobi'),

            F.sum('cnt_zhalobi_00mm').alias('cnt_zhalobi_00mm'),
            F.sum('cnt_zhalobi_01mm').alias('cnt_zhalobi_01mm'),
            F.sum('cnt_zhalobi_02mm').alias('cnt_zhalobi_02mm'),
            F.sum('cnt_zhalobi_03mm').alias('cnt_zhalobi_03mm'),
            F.sum('cnt_zhalobi_04mm').alias('cnt_zhalobi_04mm'),

            F.max('min_zhalobi_lifetime_hours').alias('min_zhalobi_lifetime_hours'),
            F.max('max_zhalobi_lifetime_hours').alias('max_zhalobi_lifetime_hours'),

            F.max('complaints_num_active_expec').alias('complaints_num_active_expec'),
            F.max('complaints_num_active').alias('complaints_num_active'),
            F.max('complaints_num_close').alias('complaints_num_close'),
            F.max('complaints_num_priority_low').alias('complaints_num_priority_low'),
            F.max('complaints_num_priority_mid').alias('complaints_num_priority_mid'),
            F.max('complaints_num_priority_lowmid').alias('complaints_num_priority_lowmid'),
            F.max('complaints_num_priority_high').alias('complaints_num_priority_high'),
            F.max('complaints_num_priority_highmid').alias('complaints_num_priority_highmid'),
            F.max('complaints_num_priority_highest').alias('complaints_num_priority_highest'),

            F.max('pay_class_00mm').alias('pay_class_00mm'),
            F.max('pay_class_01mm').alias('pay_class_01mm'),
            F.max('pay_class_02mm').alias('pay_class_02mm'),
            F.max('pay_class_03mm').alias('pay_class_03mm'),
            F.max('pay_class_04mm').alias('pay_class_04mm'),

            F.max('equipment_type_1_provide_type_1').alias('equipment_type_1_provide_type_1'),
            F.max('equipment_type_1_provide_type_2').alias('equipment_type_1_provide_type_2'),
            F.max('equipment_type_1_provide_type_3').alias('equipment_type_1_provide_type_3'),
            F.max('equipment_type_1_provide_type_4').alias('equipment_type_1_provide_type_4'),

            F.max('equipment_type_2_provide_type_1').alias('equipment_type_2_provide_type_1'),
            F.max('equipment_type_2_provide_type_2').alias('equipment_type_2_provide_type_2'),
            F.max('equipment_type_2_provide_type_3').alias('equipment_type_2_provide_type_3'),
            F.max('equipment_type_2_provide_type_4').alias('equipment_type_2_provide_type_4'),

            F.max('equipment_type_3_provide_type_1').alias('equipment_type_3_provide_type_1'),
            F.max('equipment_type_3_provide_type_2').alias('equipment_type_3_provide_type_2'),
            F.max('equipment_type_3_provide_type_3').alias('equipment_type_3_provide_type_3'),
            F.max('equipment_type_3_provide_type_4').alias('equipment_type_3_provide_type_4'),

            F.max('equipment_type_4_provide_type_1').alias('equipment_type_4_provide_type_1'),
            F.max('equipment_type_4_provide_type_2').alias('equipment_type_4_provide_type_2'),
            F.max('equipment_type_4_provide_type_3').alias('equipment_type_4_provide_type_3'),
            F.max('equipment_type_4_provide_type_4').alias('equipment_type_4_provide_type_4'),

            F.max('equipment_type_5_provide_type_1').alias('equipment_type_5_provide_type_1'),
            F.max('equipment_type_5_provide_type_2').alias('equipment_type_5_provide_type_2'),
            F.max('equipment_type_5_provide_type_3').alias('equipment_type_5_provide_type_3'),
            F.max('equipment_type_5_provide_type_4').alias('equipment_type_5_provide_type_4'),

            F.max('equipment_type_6_provide_type_1').alias('equipment_type_6_provide_type_1'),
            F.max('equipment_type_6_provide_type_2').alias('equipment_type_6_provide_type_2'),
            F.max('equipment_type_6_provide_type_3').alias('equipment_type_6_provide_type_3'),
            F.max('equipment_type_6_provide_type_4').alias('equipment_type_6_provide_type_4'),

            F.max('equipment_type_7_provide_type_1').alias('equipment_type_7_provide_type_1'),
            F.max('equipment_type_7_provide_type_2').alias('equipment_type_7_provide_type_2'),
            F.max('equipment_type_7_provide_type_3').alias('equipment_type_7_provide_type_3'),
            F.max('equipment_type_7_provide_type_4').alias('equipment_type_7_provide_type_4'),

            F.max('equipment_type_8_provide_type_1').alias('equipment_type_8_provide_type_1'),
            F.max('equipment_type_8_provide_type_2').alias('equipment_type_8_provide_type_2'),
            F.max('equipment_type_8_provide_type_3').alias('equipment_type_8_provide_type_3'),
            F.max('equipment_type_8_provide_type_4').alias('equipment_type_8_provide_type_4'),

            F.max('equipment_type_9_provide_type_1').alias('equipment_type_9_provide_type_1'),
            F.max('equipment_type_9_provide_type_2').alias('equipment_type_9_provide_type_2'),
            F.max('equipment_type_9_provide_type_3').alias('equipment_type_9_provide_type_3'),
            F.max('equipment_type_9_provide_type_4').alias('equipment_type_9_provide_type_4'),

            F.max('equipment_type_10_provide_type_1').alias('equipment_type_10_provide_type_1'),
            F.max('equipment_type_10_provide_type_2').alias('equipment_type_10_provide_type_2'),
            F.max('equipment_type_10_provide_type_3').alias('equipment_type_10_provide_type_3'),
            F.max('equipment_type_10_provide_type_4').alias('equipment_type_10_provide_type_4'),

            F.max('equipment_type_17_provide_type_1').alias('equipment_type_17_provide_type_1'),
            F.max('equipment_type_17_provide_type_2').alias('equipment_type_17_provide_type_2'),
            F.max('equipment_type_17_provide_type_3').alias('equipment_type_17_provide_type_3'),
            F.max('equipment_type_17_provide_type_4').alias('equipment_type_17_provide_type_4'),

            F.max('equipment_type_18_provide_type_1').alias('equipment_type_18_provide_type_1'),
            F.max('equipment_type_18_provide_type_2').alias('equipment_type_18_provide_type_2'),
            F.max('equipment_type_18_provide_type_3').alias('equipment_type_18_provide_type_3'),
            F.max('equipment_type_18_provide_type_4').alias('equipment_type_18_provide_type_4'),

            F.max('equipment_type_19_provide_type_1').alias('equipment_type_19_provide_type_1'),
            F.max('equipment_type_19_provide_type_2').alias('equipment_type_19_provide_type_2'),
            F.max('equipment_type_19_provide_type_3').alias('equipment_type_19_provide_type_3'),
            F.max('equipment_type_19_provide_type_4').alias('equipment_type_19_provide_type_4'),

            F.max('had_old_eq').alias('had_old_eq'),
            F.max('had_block_eq').alias('had_block_eq'),

            F.sum('active_serv_cnt').alias('active_serv_cnt'),
            F.sum('closed_cnt').alias('closed_cnt'),
            F.sum('basic_serv_cnt').alias('basic_serv_cnt'),
            F.sum('not_basic_serv_cnt').alias('not_basic_serv_cnt'),
            F.sum('all_disc_sum').alias('all_disc_sum'),

            F.max('bb_rent_amount').alias('bb_rent_amount'),
            F.max('tv_rent_amount').alias('tv_rent_amount'),
            F.max('tlf_rent_amount').alias('tlf_rent_amount'),
            F.max('tv_line_coast').alias('tv_line_coast'),
            F.max('tv_mult_coast').alias('tv_mult_coast'),

            F.first('gender', ignorenulls = True).alias('gender'),
            F.max('customer_age').alias('customer_age'),
            F.first('sale_channel', ignorenulls = True).alias('sale_channel'),

            F.sum('lk_use_count').alias('lk_use_count'),
            F.sum('all_sr_count_30d').alias('all_sr_count_30d'),
            F.sum('complaint_sr_count_30d').alias('complaint_sr_count_30d'),
            F.sum('cm_sr_count_30d').alias('cm_sr_count_30d'),
            F.sum('cm_sr_count_180d').alias('cm_sr_count_180d'),
            F.sum('lk_use_count_3m').alias('lk_use_count_3m')
        )
    )

    return final_df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("build_fix_features") \
        .master("spark://spark-master:7077") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .config("spark.cores.max", "12")\
        .config('spark.executor.memory', '6g')\
        .enableHiveSupport() \
        .getOrCreate()
    
    business_month_start = datetime(datetime.today().year, datetime.today().month, 1).date()
    business_month_end = datetime(datetime.today().year, datetime.today().month + 1, 1).date() - timedelta(days = 1)
try:
    conv = fix_conv(spark, business_month_end)
    att = fix_add_att(spark, business_month_end)
    pay = fix_add_pay(spark, business_month_end)
    traffic = fix_add_traffic(spark, business_month_end)
    equip = fix_add_equip(spark, business_month_end)
    remedy = fix_add_remedy(spark, business_month_end, conv)
    serv = fix_add_serv(spark, business_month_end)
    gr = fix_add_gr(spark)
    
    union_df = (
        conv
        .join(att, ['billing_id', 'acc_id'], 'left')
        .join(pay, ['billing_id', 'acc_id'], 'left')
        .join(traffic, ['billing_id', 'acc_id'], 'left')
        .join(equip, ['billing_id', 'acc_id'], 'left')
        .join(remedy, ['billing_id', 'acc_id', 'acc_num', 'contract_num'], 'left')
        .join(serv, ['billing_id', 'acc_id'], 'left')
        .join(gr, ['billing_id', 'customer_id'], 'left')
    )
    
    final_df = fix_final_df(union_df, business_month_end)

    logger.info('saving_table')

    (
        final_df
        .repartition(3)
        .write
        .mode("overwrite")
        .format("orc")
        .saveAsTable("spfix_dm.fix_features")
    )
except Exception as e:
        logger.error("Failed to save table", exc_info=True)
        raise
finally:
    spark.stop()