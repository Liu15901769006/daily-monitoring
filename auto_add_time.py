#-*- coding: utf-8 -*-

import glob
import os
import sys
import datetime
import calendar
import pandas as pd
import time

from ks_trend import (observe_and_score_write_ks,
                                    observe_and_score_write_ks_file,
                                    observe_and_score_write_ks_oracle,
                                    observe_and_score_write_ks_spark
                                   )
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("lzh_desc").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

def add_months(dt, months):
    month = dt.month - 1 + months
    year = dt.year + int(month / 12)
    month = month % 12 + 1
    day = min(dt.day, calendar.monthrange(year, month)[1])
    return dt.replace(year=year, month=month, day=day)

def getbetweenmonth(begin_date, end_date):
    date_list = []
    while begin_date <= end_date:
        period = []
        month_start = begin_date.strftime("%Y-%m-01")
        period.append(month_start)

        last_day = calendar.monthrange(begin_date.year, begin_date.month)[1]
        month_end = begin_date.strftime("%Y-%m-") + str(last_day)
        period.append(month_end)
        date_list.append(period)

        begin_date = add_months(begin_date, 1)
    return date_list


def ex_observe_and_score_write_ks_files_by_spark_180718(y_lst, score_model, product_lst, time_lst):
    """ 从 spark 中得到 ks， 这里接受的是 0-1000分
        需要 spark2-submit 执行
    """

    for y in y_lst:
        for score_name in score_model:
            for product in product_lst:
                for time1, time2 in time_lst:

                    x = """select %(y)s as observe, %(nscore)s as score
                from tmp.score_union_all
                where to_date(loan_date) between '%(time1)s' and '%(time2)s'
                and loankind = '%(product)s'
                and %(y)s is not null
                and %(nscore)s is not null""" % {
                    "y": y,
                    "nscore":score_name[0],
                    "model": score_name[1],
                    "product": product,
                    "time1": time1,
                    "time2": time2
                }
                    print x

                    name = "%(model)s_model-%(product)s-%(y)s_y-%(time1)s-%(time2)s" % {
                    "y": y,
                    "model": score_name[1],
                    "product": product.split("/")[1],
                    "time1": time1,
                    "time2": time2
                }
                    try:
                        observe_and_score_write_ks_spark(x, spark, outputfile="./lzh/ks/output/%s.xlsx" % name, download=True)
                    except:
                        print "error"

if __name__ == "__main__":
    y_lst = ["fst_bill_ovrd", "fst_bill_a3_ovrd","fst_bill_a5_ovrd", "fst_bill_a7_ovrd", "fst_bill_a10_ovrd", "fst_bill_a15_ovrd","fst_bill_a20_ovrd","fst_bill_a40_ovrd","fst_3_zero_pay","third_m2","forth_m2","everm2"]
    
    score_model = [
                    ["ccl_ext_v1_score", "ccl_ext_v1"], 
                    ["jxl_v1_score", "jxl_v1"], 
                    ["ddq_mob_as_v0925_score", "ddq_mob"], 
                    ["kkd_mob_as_v0925_score", "kkd_mob"]
                  ]

    df = spark.sql("""select loankind, max(loan_date) as mdate
                  from tmp.score_union_all
                  group by loankind
                  having year(mdate) = 2018 """).toPandas()
    product_lst = df["loankind"].tolist()


    end_date = datetime.date.today()
    begin_date = add_months(end_date, -5)
    time_lst = getbetweenmonth(begin_date, end_date)
    
    ex_observe_and_score_write_ks_files_by_spark_180718(y_lst, score_model, product_lst, time_lst)
