from pyspark.sql import SparkSession
import pandas
spark = SparkSession.builder.appName("lzh_desc").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

spark.sql("""insert overwrite table tmp.ccl_ks
             select *
             from tmp.ccl_ks
             where date_format(loan_date, 'YYYY-MM') != date_format(add_months(current_date, -1), 'YYYY-MM')""")

spark.sql("""insert overwrite table tmp.jxl_ks
             select *
             from tmp.jxl_ks
             where date_format(loan_date, 'YYYY-MM') != date_format(add_months(current_date, -1), 'YYYY-MM') """)

spark.sql("""insert overwrite table tmp.ddq_mob_ks
             select *
             from tmp.ddq_mob_ks
             where date_format(loan_date, 'YYYY-MM') != date_format(add_months(current_date, -1), 'YYYY-MM') """)

spark.sql("""insert overwrite table tmp.kkd_mob_ks
             select *
             from tmp.kkd_mob_ks
             where date_format(loan_date, 'YYYY-MM') != date_format(add_months(current_date, -1), 'YYYY-MM') """)

spark.sql("""insert into table tmp.ccl_ks
select  bid, loan_date, loankind, ccl_ext_v1_score
from (select a.bid, a.loan_date, a.loankind, b.ccl_ext_v1_score, 
row_number() over(partition by a.bid order by b.dt desc, b.report_id desc) rn,
date_format(loan_date, 'YYYY-MM') as ym
from rdm.mod_y_ln a
left join rdm.ccl_ext_v1_score b
on a.cert_no = b.cert_no
where to_date(a.loan_date) >= b.dt
and add_months (b.dt, 3) >= to_date(a.loan_date)
)t
where t.rn = 1
and t.ym = date_format(add_months(current_date, -1), 'YYYY-MM')
""")

spark.sql("""insert into table tmp.jxl_ks
select bid, loan_date, loankind, jxl_v1_score
from(
select a.bid, a.loan_date, a.loankind, b.jxl_v1_score,
row_number() over(partition by a.bid order by b.request_dt desc) rn,
date_format(loan_date, 'YYYY-MM') as ym
from rdm.mod_y_ln a 
left join 
rdm.jxl_v1_score b 
on a.cert_no = b.cert_no and a.mobile = b.mobile
where to_date(a.loan_date) >= to_date(b.request_dt)
)t
where t.rn = 1
and t.ym  = date_format(add_months(current_date, -1), 'YYYY-MM')"""
)

spark.sql("""insert into table tmp.ddq_mob_ks
select bid, loan_date, loankind, ddq_mob_as_v0925_score
from (select a.bid, a.loan_date, a.loankind, b.ddq_mob_as_v0925_score,
row_number() over(partition by a.bid order by b.dt desc, b.basic_id desc) rn,
date_format(loan_date, 'YYYY-MM') as ym
from rdm.mod_y_ln a 
left join 
rdm.ddq_mob_as_v0925_score b 
on a.mobile = b.mobile
where to_date(a.loan_date) >= b.dt
and add_months (b.dt, 3) >= to_date(a.loan_date)
)t
where t.rn = 1
and t.ym = date_format(add_months(current_date, -1), 'YYYY-MM')"""
)

spark.sql("""insert into table tmp.kkd_mob_ks
select bid, loan_date, loankind, kkd_mob_as_v0925_score
from (select a.bid, a.loan_date, a.loankind, b.kkd_mob_as_v0925_score,
row_number() over(partition by a.bid order by b.dt desc, b.basic_id desc) rn,
date_format(loan_date, 'YYYY-MM') as ym
from rdm.mod_y_ln a 
left join 
rdm.kkd_mob_as_v0925_score b 
on a.mobile = b.mobile
where to_date(a.loan_date) >= b.dt
and add_months (b.dt, 3) >= to_date(a.loan_date)
)t
where t.rn = 1
and t.ym = date_format(add_months(current_date, -1), 'YYYY-MM')"""
)

spark.sql("""drop table tmp.score_union_all""")

spark.sql("""create table tmp.score_union_all
as 
select a.bid,  a.loankind, a.loan_date, a.fst_bill_ovrd, a.fst_bill_a3_ovrd, a.fst_bill_a5_ovrd, a.fst_bill_a7_ovrd, a.fst_bill_a10_ovrd, 
a.fst_bill_a15_ovrd, a.fst_bill_a20_ovrd, a.fst_bill_a40_ovrd, a.fst_3_zero_pay, a.third_m2, a.forth_m2, a.everm2, b.ccl_ext_v1_score,
c.jxl_v1_score, d.ddq_mob_as_v0925_score, e.kkd_mob_as_v0925_score
from rdm.mod_y_ln a
left join tmp.ccl_ks b
on a.bid = b.bid
left join tmp.jxl_ks c 
on a.bid = c.bid 
left join tmp.ddq_mob_ks d 
on a.bid = d.bid
left join tmp.kkd_mob_ks e 
on a.bid = e.bid""")



