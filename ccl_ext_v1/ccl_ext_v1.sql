# 建表语句
drop table tmp.ccl_ks;
create table tmp.ccl_ks
as 
select bid, loan_date, loankind, ccl_ext_v1_score
from (select a.bid, a.loan_date, a.loankind, b.ccl_ext_v1_score, 
row_number() over(partition by a.bid order by b.dt desc, b.report_id desc) rn
from tmp.mod_y_ln_180702 a
left join rdm.ccl_ext_v1_score b
on a.cert_no = b.cert_no
where to_date(a.loan_date) >= b.dt
and add_months (b.dt, 3) >= to_date(a.loan_date)
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
select  loankind, substr(loan_date,1,7), count(*)
from tmp.ccl_ks
group by loankind, substr(loan_date,1,7)
order by loankind, substr(loan_date,1,7)
;


