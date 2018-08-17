#-*- coding: utf-8 -*-

import glob
import os
import sys


from ks_trend import (observe_and_score_write_ks,
                                    observe_and_score_write_ks_file,
                                    observe_and_score_write_ks_oracle,
                                    observe_and_score_write_ks_spark
                                   )


def ex_observe_and_score_write_ks_files_by_spark_180622():
    """ 从 spark 中得到 ks， 这里接受的是 0-1000分
        需要 spark2-submit 执行
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("lzh_desc").getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    y_lst = ["fst_bill_ovrd", "fst_bill_a3_ovrd", "fst_bill_a5_ovrd", "fst_bill_a7_ovrd", "fst_bill_a10_ovrd", "fst_bill_a15_ovrd","fst_bill_a20_ovrd","fst_bill_a40_ovrd","fst_3_zero_pay","third_m2","forth_m2","everm2"]
    model_lst = ["jxl_v1"]
    product_lst = [
	                "LOANKIND/AOPAIFANGYIDAI",
	                "LOANKIND/BENKEDAI",
	                "LOANKIND/DAEHUA",
	                "LOANKIND/DOUDOUQIAN",
	                "LOANKIND/FANGYIDAIER",
	                "LOANKIND/HUIYUANFEI",
	                "LOANKIND/JIEQUHUA",
	                "LOANKIND/JIEXIANJIN",
	                "LOANKIND/JINXINDAI",
	                "LOANKIND/KAKADAI",
	                "LOANKIND/KUAIDAI",
	                "LOANKIND/LIRENDAI",
	                "LOANKIND/MIAOFENQI",
	                "LOANKIND/NEWGONGXINDAI",
	                "LOANKIND/SUIYIHUA",
	                "LOANKIND/TIANYICHENGFENQI",
	                "LOANKIND/TIANYIXIANJINDAI",
	                "LOANKIND/TYBT",
	                "LOANKIND/WEILIDAI2018",
	                "LOANKIND/XINGXINGFENQIGOU",
	                "LOANKIND/XINLOUDAI",
	                "LOANKIND/XUELIDAI"
	            ]
    time_lst = [
                ["2018-03-01","2018-03-31"],
                ["2018-04-01","2018-04-30"],
                ["2018-05-01","2018-05-31"]
                ]

    for y in y_lst:
        for model in model_lst:
            for product in product_lst:
                for time1, time2 in time_lst:

                    x = """select %(y)s as observe, score
                    from tmp.jxl_ks
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
                        observe_and_score_write_ks_spark(x, spark, outputfile="output/%s.xlsx" % name, download=True)
                    except:
                        print "error"

if __name__ == "__main__":
    ex_observe_and_score_write_ks_files_by_spark_180622()
