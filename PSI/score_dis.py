# -*- coding = utf-8 -*-
import pandas as pd
from sqlalchemy import create_engine, MetaData
import os
import numpy as np


def getDis(excel_name, model_name):
    df = pd.read_excel(excel_name)
    df['model'] = model_name
    df.columns = ['loankind', 'period', 'sep', 'cum', 'total', 'dis', 'model']
    df = df[['model', 'loankind', 'period', 'sep', 'cum', 'total', 'dis']]
    return df


if __name__ == "__main__":
    name_lst = [['ccl_dis.xlsx', 'ccl_ext_v1_model'],
                ['ddq_dis.xlsx', 'ddq_mob_model'],
                ['jxl_dis.xlsx', 'jxl_v1_model'],
                ['kkd_dis.xlsx', 'kkd_mob_model']]
    df_union = getDis(name_lst[0][0], name_lst[0][1])
    for name in name_lst[1:]:
        tmp_df = getDis(name[0], name[1])
        df_union = pd.concat([df_union, tmp_df])

    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    engine = create_engine("postgresql+psycopg2://internship:internship.wx@10.138.61.40:5432/test", echo=True)

    state = pd.read_sql("select distinct model, loankind, period from lzh_score_dis", engine)
    state = np.array(state).tolist()
    tab_has = []
    for i in range(0, len(df_union)):
        a = (df_union.iloc[i][:3]).values.tolist()
        if a in tab_has:
            pass
        else:
            tab_has.append(a)
    for tab in tab_has:
        if tab in state:
            engine.execute("delete from lzh_score_dis where model = '{0[0]}' and loankind = '{0[1]}' and "
                           "period = '{0[2]}'".format(tab), engine)

    df_union.to_sql("lzh_score_dis", engine, if_exists="append", index=False)




