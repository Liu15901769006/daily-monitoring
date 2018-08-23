# -*- coding = utf-8 -*-

import pandas as pd
import itertools
import numpy as np
from sqlalchemy import create_engine, MetaData
import os


def getPsi(excel_name, score_name, model_name):
    df = pd.read_excel(excel_name)

    loankind_lst = list(df['loankind'].unique())
    period_lst = list(df['period'].unique())
    per_combin = list(itertools.combinations(period_lst, 2))
    sep_lst = ['260以下', '[260,280)', '[280,300)', '[300,320)', '[320,340)', '[340,360)', '[360,380)', '[380,400)',
               '[400,420)', '[420,440)', '[440,460)', '[460,480)', '[480,500)', '[500,520)', '[520,540)', '[540,560)',
               '[560,580)', '[580,600)', '[600,620)', '[620,640)', '[640,660)', '[660,680)', '[680,700)', '[700,720)',
               '720以上']

    union_psi = []

    for loankind in loankind_lst:
        for per in per_combin:
            x = df[(df['loankind'] == loankind) & (df['period'] == per[0])]
            y = df[(df['loankind'] == loankind) & (df['period'] == per[1])]
            if x.empty or y.empty:
                pass
            else:
                x_dis = dict(x[[score_name, 'dis']].values)
                y_dis = dict(y[[score_name, 'dis']].values)

                psi = 0
                exist = 0
                for sep in sep_lst:
                    if (sep in x_dis.keys()) & (sep in y_dis.keys()):
                        psi += (x_dis[sep] - y_dis[sep]) * np.log(x_dis[sep] / y_dis[sep])
                        exist = 1
                if exist == 1:
                    tmp_lst = [loankind, per[0], per[1], psi]
                    union_psi.append(tmp_lst)
    model_psi = pd.DataFrame(union_psi, columns=['loankind', 'period_a', 'period_b', 'psi'])
    model_psi['model'] = model_name
    model_psi = model_psi[['model', 'loankind', 'period_a', 'period_b', 'psi']]
    return model_psi


if __name__ == "__main__":
    name_lst = [['ccl_dis.xlsx', 'ccl_ext_v1_score', 'ccl_ext_v1_model'],
                ['ddq_dis.xlsx', 'ddq_mob_as_v0925_score', 'ddq_mob_model'],
                ['jxl_dis.xlsx', 'jxl_v1_score', 'jxl_v1_model'],
                ['kkd_dis.xlsx', 'kkd_mob_as_v0925_score', 'kkd_mob_model']]
    psi_union = getPsi(name_lst[0][0], name_lst[0][1], name_lst[0][2])
    for name in name_lst[1:]:
        tmp_psi = getPsi(name[0], name[1], name[2])
        psi_union = pd.concat([psi_union, tmp_psi], ignore_index=True)

    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    engine = create_engine("postgresql+psycopg2://internship:internship.wx@10.138.61.40:5432/test", echo=True)
    state = pd.read_sql("select distinct model, loankind, period_a, period_b from lzh_score_psi", engine)
    state = np.array(state).tolist()
    tab_has = []
    for i in range(0, len(psi_union)):
        a = (psi_union.iloc[i][:4]).values.tolist()
        if a in tab_has:
            pass
        else:
            tab_has.append(a)
    for tab in tab_has:
        if tab in state:
            engine.execute("delete from lzh_score_psi where model = '{0[0]}' and loankind = '{0[1]}' and "
                           "period_a = '{0[2]}' and period_b = '{0[3]}'".format(tab), engine)
    psi_union.to_sql('lzh_score_psi', engine, if_exists="append", index=False)









