模型KS追踪
一、、涉及的表
模型宽表tmp.mod_y_ln_180702，重点关注业务号bid和y指标；
模型宽表rdm.mod_y_ln（数据不断更新），表结构与tmp.mod_y_ln_180702相同；
卡卡征信分表rdm.ccl_ext_v1_score，重点关注ccl_ext_v1_score；
聚信立分表rdm.jxl_v1_score，重点关注jxl_v1_score；
卡卡手机分表rdm.kkd_mob_as_v0925_score，重点关注rdm.kkd_mob_as_v0925_score；
豆豆手机分表rdm.ddq_mob_as_v0925_score，重点关注rdm.ddq_mob_as_v0925_score。

二、表连接过程
将模型宽表分别与卡卡征信分表、卡卡手机分表、豆豆手机分表和聚信立分表关联，依次得到表tmp.ccl_ks、tmp.kkd_mob_ks、tmp.ddq_mob_ks和tmp.jxl_ks。以上四张表的结构均是业务号bid,放贷时间loan_date,放贷产品loankind,分数。
关联规则相同之处在于：模型宽表中的放贷时间loan_date要晚于分数表中的打分时间dt，而且相差时间不超过三个月。
不同之处在于：模型宽表与卡卡征信分表通过身份证号cert_no关联，模型宽表与聚信立分表通过身份证号cert_no和手机号mobile关联，其余两个均通过手机号mobile关联。
为实现简化调用过程，将tmp.ccl_ks、tmp.kkd_mob_ks、tmp.ddq_mob_ks和tmp.jxl_ks合并成一张表tmp.score_union_all，该表包含内容主要有业务号、y指标、四种分数等。

三、表数据更新
1、解决思路：更新四张子表，即tmp.ccl_ks、tmp.kkd_mob_ks、tmp.ddq_mob_ks和tmp.jxl_ks,再重新执行tmp.score_union_all建表语句。注意此时建表语句涉及的模型宽表是rdm.mod_y_ln，因其含有最新的y指标数据。
2、具体说明更新子表过程。
以tmp.ccl_ks为例，基于关联规则，建立rdm.mod_y_ln与rdm.ccl_ext_v1_score的连接，挑选上一月份（以当前时间为基准）数据插入tmp.ccl_ks。月份是动态获取。
3、若多次进行数据插入操作，会造成数据重复现象。为避免出现该问题，在插入新数据之前，进行insert overwrite操作。
将更新过程写成auto_update.py文件，需要更新数据时直接运行该程序即可，运行平台是shell。

四、运次程序
由于历史数据已有KS追踪结果，只需运行增量代码auto_add_time.py。运行平台是shell，用crontab设定每月1号9点定时运行该程序。运行结果需要下载到本地，再用get_batch_ks_file-jys程序处理，得到汇总结果。
增量代码会动态获取产品列表和时间列表。运行前不需要修改该程序。产品列表中的元素均是2018年存在放贷现象的产品。

五、结果插入pg数据库
运行pgSQL.py（只需更改读入文件的路径），可将get_batch_ks_file-jys程序结果插入pg数据库中。
pgSQL.py运行环境是python3.5。
数据将分别保存至lzh_score_ks和lzh_score_group两张表中，表结构参见数据字典。
