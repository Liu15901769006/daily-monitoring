#-*- coding: utf-8 -*-

import glob
import os
import sys
import datetime
import calendar
import pandas as pd
import time
from pyspark.sql import SparkSession

from ks_trend import (observe_and_score_write_ks,
                                    observe_and_score_write_ks_file,
                                    observe_and_score_write_ks_oracle,
                                    observe_and_score_write_ks_spark
                                   )


spark = SparkSession.builder.appName("lzh_desc").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

def ex_observe_and_score_write_ks_files_by_spark_180809(y_lst, score_model, product_lst, time_lst):
    """ 从 spark 中得到 ks， 这里接受的是 0-1000分
        需要 spark2-submit 执行
    """

    for y in y_lst:
        for model in [score_model[1]]:
            for product in product_lst:
                for time1, time2 in time_lst:
            
                    x = """select %(y)s as observe, score
                from tmp.tmp_task_score
                where to_date(loan_date) between '%(time1)s' and '%(time2)s'
                and loankind = '%(product)s'
                and %(y)s is not null
                and score is not null""" % {
                    "y": y,
                    "model": model,
                    "product": product,
                    "time1": time1,
                    "time2": time2
                }
                    print x
            
                    name = "%(model)s_model-%(product)s-%(y)s_y-%(time1)s-%(time2)s" % {
                    "y": y,
                    "model": model,
                    "product": product.split("/")[1],
                    "time1": time1,
                    "time2": time2
                }
                    try:
                        observe_and_score_write_ks_spark(x, spark, outputfile="output2/%s.xlsx" % name, download=True)
                    except:
                        print "error"

if __name__ == "__main__":
    """
    根据需求，修改y_lst(y指标)、score_model(模型分、模型名称、表名)、product_lst(产品列表)、
    time_lst(时间列表)以及建表语句
     """
    y_lst = ["fst_bill_ovrd", "fst_bill_a3_ovrd","fst_bill_a5_ovrd", "fst_bill_a7_ovrd", "fst_bill_a10_ovrd",
    "fst_bill_a15_ovrd","fst_bill_a20_ovrd","fst_bill_a40_ovrd","fst_3_zero_pay","third_m2","forth_m2","everm2"]

    score_model_all = [
                    ["ccl_ext_v1_score", "ccl_ext_v1", "rdm.ccl_ext_v1_score"], 
                    ["jxl_v1_score", "jxl_v1", "rdm.jxl_v1_score"], 
                    ["ddq_mob_as_v0925_score", "ddq_mob", "rdm.ddq_mob_as_v0925_score"], 
                    ["kkd_mob_as_v0925_score", "kkd_mob", "rdm.kkd_mob_as_v0925_score"]
                  ]
    score_model = score_model_all[2]

    product_lst = [
                   "LOANKIND/DOUDOUQIAN",
                   "LOANKIND/KAKADAI"
                  ]

    time_lst = [
               # ["2018-01-01", "2018-01-31"],
               # ["2018-02-01", "2018-02-28"],
               # ["2018-03-01", "2018-03-31"],
               # ["2018-04-01", "2018-04-30"],
               # ["2018-05-01", "2018-05-31"],
               ["2018-06-01", "2018-06-30"]
               ]

    spark.sql("""drop table tmp.tmp_data""")
    spark.sql("""drop table tmp.tmp_task_score""")
    spark.sql("""create table tmp.tmp_data as
               select a.bid, a.loankind, a.loan_date, a.fst_bill_ovrd, a.fst_bill_a3_ovrd, a.fst_bill_a5_ovrd,
               a.fst_bill_a7_ovrd, a.fst_bill_a10_ovrd, a.fst_bill_a15_ovrd, a.fst_bill_a20_ovrd, a.fst_bill_a40_ovrd, 
               a.fst_3_zero_pay, a.third_m2, a.forth_m2, a.everm2, b.%s as score,
               row_number() over(partition by a.bid order by b.dt desc, b.basic_id desc) rn
               from rdm.mod_y_ln a
               left join
               %s b
               on a.mobile = b.mobile
               where to_date(a.loan_date) >= b.dt
               and add_months (b.dt, 3) >= to_date(a.loan_date)
               """ % (
               score_model[0], score_model[2]
                 ))
    spark.sql("""create table tmp.tmp_task_score
            as 
            select bid, loankind, loan_date, fst_bill_ovrd, fst_bill_a3_ovrd, fst_bill_a5_ovrd, fst_bill_a7_ovrd,
                   fst_bill_a10_ovrd, fst_bill_a15_ovrd, fst_bill_a20_ovrd, fst_bill_a40_ovrd, fst_3_zero_pay,
                   third_m2, forth_m2, everm2, score
            from tmp.tmp_data t
            where t.rn = 1""")

    ex_observe_and_score_write_ks_files_by_spark_180809(y_lst, score_model, product_lst, time_lst)
