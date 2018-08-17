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
    model_lst = ["ccl_ext_v1"]
    product_lst = [
	                "LOANKIND/ANJIAQUHUA",
                    "LOANKIND/BENKEDAI",
                    "LOANKIND/DOUDOU",
                    "LOANKIND/DOUDOUHUA",
                    "LOANKIND/DOUDOUQIAN",
                    "LOANKIND/FANGYIDAIER",
                    "LOANKIND/GAOXINDAI",
                    "LOANKIND/GOUJIBAO",
                    "LOANKIND/HEBAOXIANJINDAI",
                    "LOANKIND/HUANLE",
                    "LOANKIND/HUIYUANFEI",
                    "LOANKIND/JIANLIDAI",
                    "LOANKIND/JIELEHUA",
                    "LOANKIND/JIEQUHUA",
                    "LOANKIND/JIEXIANJIN",
                    "LOANKIND/JINXINDAI",
                    "LOANKIND/KAKADAI",
                    "LOANKIND/KAKALHQ",
                    "LOANKIND/KUAIDAI",
                    "LOANKIND/LIRENDAI",
                    "LOANKIND/LIRENHUI",
                    "LOANKIND/LOUERDAI",
                    "LOANKIND/MIAOFENDAI",
                    "LOANKIND/MIAOFENLYFQ",
                    "LOANKIND/MIAOFENQI",
                    "LOANKIND/MIAOFENQIANBAO",
                    "LOANKIND/NEWGONGXINDAI",
                    "LOANKIND/NEWLOUYIDAI",
                    "LOANKIND/RONGYIDAI",
                    "LOANKIND/SHBT",
                    "LOANKIND/SHOUJIDAI",
                    "LOANKIND/SUIYIHUA",
                    "LOANKIND/TIANYICHENGFENQI",
                    "LOANKIND/TIANYIXIANJINDAI",
                    "LOANKIND/TYBT",
                    "LOANKIND/WEILIDAI",
                    "LOANKIND/WEILIDAI2018",
                    "LOANKIND/XINGXINGFENQIGOU",
                    "LOANKIND/XINGXINGQIANDAI",
                    "LOANKIND/XINLOUDAI",
                    "LOANKIND/XINYIDAI",
                    "LOANKIND/XINYONGHUA",
                    "LOANKIND/XUELIDAI",
                    "LOANKIND/XYHFENQI",
                    "LOANKIND/YONGJINFENQI"
	]
    time_lst = [
                ["2016-07-01","2016-07-31"],
                ["2016-08-01","2016-08-31"],
                ["2016-09-01","2016-09-30"],
                ["2016-10-01","2016-10-31"],
                ["2016-11-01","2016-11-30"],
                ["2016-12-01","2016-12-31"],
                ["2017-01-01","2017-01-31"],
                ["2017-02-01","2017-02-28"],
                ["2017-03-01","2017-03-31"],
                ["2017-04-01","2017-04-30"],
                ["2017-05-01","2017-05-31"],
                ["2017-06-01","2017-06-30"],
                ["2017-07-01","2017-07-31"],
                ["2017-08-01","2017-08-31"],
                ["2017-09-01","2017-09-30"],
                ["2017-10-01","2017-10-31"],
                ["2017-11-01","2017-11-30"],
                ["2017-12-01","2017-12-31"],
                ["2018-01-01","2018-01-31"],
                ["2018-02-01","2018-02-28"],
                ["2018-03-01","2018-03-31"],
                ["2018-04-01","2018-04-30"],
                ["2018-05-01","2018-05-31"]
                ]

    for y in y_lst:
        for model in model_lst:
            for product in product_lst:
                for time1, time2 in time_lst:

                    x = """select observe, score
                    from (select %(y)s as observe, ccl_ext_v1_score as score,
                    row_number() over(partition by bid order by dt desc, report_id desc) rn
                    from tmp.ks
                    where to_date(loan_date) between '%(time1)s' and '%(time2)s'
                    and loankind = '%(product)s'
                    ) t
                    where t.rn = 1
                    and observe is not null
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
