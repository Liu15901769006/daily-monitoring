# 建表语句
drop table tmp.kkd_mob_ks;
create table tmp.kkd_mob_ks
as 
select bid, loan_date, loankind, kkd_mob_as_v0925_score
from (select a.bid, a.loan_date, a.loankind, b.kkd_mob_as_v0925_score,
row_number() over(partition by a.bid order by b.dt desc, b.basic_id desc) rn
from tmp.mod_y_ln_180702 a 
left join 
rdm.kkd_mob_as_v0925_score b 
on a.mobile = b.mobile
where to_date(a.loan_date) >= b.dt
and add_months (b.dt, 3) >= to_date(a.loan_date)
)t
where t.rn = 1
;

# 放贷量
select loankind, substr(loan_date,1,7), count(*)
from tmp.mod_y_ln_180702
group by loankind, substr(loan_date,1,7)
order by loankind, substr(loan_date,1,7)
;

# 样本量
select loankind, substr(loan_date,1,7), count(*)
from tmp.kkd_mob_ks
group by loankind, substr(loan_date,1,7)
order by loankind, substr(loan_date,1,7)
;

