# 建表语句
drop table tmp.jxl_ks;
create table tmp.jxl_ks
as 
select bid, loan_date, loankind, jxl_v1_score
from(
select a.bid, a.loan_date, a.loankind, b.jxl_v1_score,
row_number() over(partition by a.bid order by b.request_dt desc) rn
from tmp.mod_y_ln_180702 a 
left join 
rdm.jxl_v1_score b 
on a.cert_no = b.cert_no and a.mobile = b.mobile
where to_date(a.loan_date) >= to_date(b.request_dt)
)t
where t.rn = 1
;

# 放款量
select loankind, substr(loan_date,1,7), count(*)
from tmp.mod_y_ln_180702
group by loankind, substr(loan_date,1,7)
order by loankind, substr(loan_date,1,7)
;

# 样本量
select loankind, substr(loan_date,1,7), count(*)
from tmp.jxl_ks
group by loankind, substr(loan_date,1,7)
order by loankind, substr(loan_date,1,7)
;

