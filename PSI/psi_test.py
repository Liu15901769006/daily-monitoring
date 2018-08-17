# -*- coding: utf-8 -*-

from sqlalchemy import create_engine, MetaData
import pandas as pd
import numpy as np
import os
import itertools


os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
engine = create_engine("postgresql+psycopg2://internship:internship.wx@10.138.61.40:5432/test", echo=True)

model_lst = pd.read_sql("select DISTINCT model from lzh_score_group", engine)["model"].tolist()
loankind_lst = pd.read_sql("select DISTINCT loankind from lzh_score_group order by loankind", engine)["loankind"].tolist()
y_lst = pd.read_sql("select DISTINCT y_indicator from lzh_score_group order by y_indicator", engine)["y_indicator"].tolist()
period_lst = pd.read_sql("select DISTINCT period from lzh_score_group order by period", engine)["period"].tolist()
per_combin = itertools.combinations(period_lst, 2)

final_result = []

for model in model_lst:
    for loankind in loankind_lst:
        for y in y_lst:
            for per in per_combin:
                a = "select DISTINCT division ,distribution from lzh_score_group where model = '%s'" \
                " and loankind = '%s' and y_indicator = '%s' and period = '%s' order by division" % (model, loankind, y, per[0])
                b = "select DISTINCT division ,distribution from lzh_score_group where model = '%s'" \
                " and loankind = '%s' and y_indicator = '%s' and period = '%s' order by division" % (model, loankind, y, per[1])

                a_dis = pd.read_sql(a, engine)["distribution"].tolist()
                b_dis = pd.read_sql(b, engine)["distribution"].tolist()

                psi = 0
                if len(a_dis) & len(b_dis):
                    for i in range(len(a_dis)):
                        if (a_dis[i] != 0) & (b_dis[i] != 0):
                            psi += (a_dis[i] - b_dis[i]) *np.log(a_dis[i] / b_dis[i])
                    result = [model, loankind, y, per[0], per[1], psi]
                    final_result.append(result)

psi_df = pd.DataFrame(final_result, columns=['model', 'loankind', 'y_indicator', 'period_a', 'period_b', 'psi'])
psi_df.to_sql("lzh_psi", engine, index=False)