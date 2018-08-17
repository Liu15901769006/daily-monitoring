# -*- coding: utf-8 -*-

from sqlalchemy import create_engine, MetaData
import pandas as pd
import os
import time
import psycopg2
import numpy as np

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
engine = create_engine("postgresql+psycopg2://internship:internship.wx@10.138.61.40:5432/test", echo=True)

path = u"E:/模型分回溯/聚信立分"
files = os.listdir(path)
union_value = []
ks_all = pd.DataFrame()
group = pd.DataFrame()
flag = int(pd.read_sql("select max(id) from lzh_score_ks", engine)["max"] + 1)
# flag = 1

state = pd.read_sql("select distinct model, loankind, y_indicator, period from lzh_score_ks", engine)
state = np.array(state).tolist()

for file in files:
    result = file.split('-')
    result[2] = result[2].replace("_y", "", 1)
    result[-1] = result[-1].split('.')[0]
    period = "/".join(result[3:5])

    del result[3:]
    result.append(period)
    result.append(time.strftime("%Y/%m/%d", time.localtime(time.time())))
    result.append(flag)

    if result[:4] in state:
        engine.execute("delete from lzh_score_ks where model = '{0[0]}'"
                    " and loankind = '{0[1]}' and y_indicator = '{0[2]}'"
                    " and period = '{0[3]}' ".format(result), engine)

    df = pd.read_excel(path + '/' + file)
    result.append(abs(df['KS']).max())
    union_value.append(result)

    df['id'] = flag
    ks_all = ks_all.append(df)

    gdf = pd.read_excel(path + '/' + file,sheetname=1)
    gdf['id'] = flag
    group = group.append(gdf)

    flag += 1

df1 = pd.DataFrame(union_value, columns=['model', 'loankind', 'y_indicator', 'period', 'time', 'id', 'ks'])

ks_all = ks_all.reset_index(drop=True)
ks_all.columns = ['lowest_score', 'highest_score', 'total_num', 'default_num', 'default_rate', 'non_default',
                  'cum_default', 'cum_non_default', 'ks_frag', 'lifting', 'cum_lifting', 'id']

ks_result = pd.merge(df1, ks_all, on=['id'])

ks_result.to_sql("lzh_score_ks", engine, if_exists="append", index=False)
# print(pd.read_sql("select * from lzh_score_ks", engine))


group = group.reset_index(drop=True)
group.columns = ['division', 'total', 'default', 'default_rate', 'distribution', 'cum_dis', 'id']
group_result = pd.merge(df1, group, on=['id'])

group_result.to_sql("lzh_score_group", engine, if_exists="append", index=False)
# print(pd.read_sql("select * from lzh_score_group", engine))













