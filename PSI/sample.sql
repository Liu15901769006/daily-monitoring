# 得到分数的分布（产品名称，月份，分数段，分布）
# ccl征信分

select m.loankind, m.period, ccl_ext_v1_score, cum, total, cum/total as dis
from 
(
select loankind, period, ccl_ext_v1_score, count(*) as cum
from
(select split_part(loankind, "/", 2)as loankind, substr(loan_date,1,7)as period, 
case when ccl_ext_v1_score <260 then '260以下'
     when 260 <= ccl_ext_v1_score and ccl_ext_v1_score < 280 then  '[260,280)'
     when 280 <= ccl_ext_v1_score and ccl_ext_v1_score < 300 then  '[280,300)'
     when 300 <= ccl_ext_v1_score and ccl_ext_v1_score < 320 then  '[300,320)'
     when 320 <= ccl_ext_v1_score and ccl_ext_v1_score < 340 then  '[320,340)'
     when 340 <= ccl_ext_v1_score and ccl_ext_v1_score < 360 then  '[340,360)'
     when 360 <= ccl_ext_v1_score and ccl_ext_v1_score < 380 then  '[360,380)'
     when 380 <= ccl_ext_v1_score and ccl_ext_v1_score < 400 then  '[380,400)'
     when 400 <= ccl_ext_v1_score and ccl_ext_v1_score < 420 then  '[400,420)'
     when 420 <= ccl_ext_v1_score and ccl_ext_v1_score < 440 then  '[420,440)'
     when 440 <= ccl_ext_v1_score and ccl_ext_v1_score < 460 then  '[440,460)'
     when 460 <= ccl_ext_v1_score and ccl_ext_v1_score < 480 then  '[460,480)'
     when 480 <= ccl_ext_v1_score and ccl_ext_v1_score < 500 then  '[480,500)'
     when 500 <= ccl_ext_v1_score and ccl_ext_v1_score < 520 then  '[500,520)'
     when 520 <= ccl_ext_v1_score and ccl_ext_v1_score < 540 then  '[520,540)'
     when 540 <= ccl_ext_v1_score and ccl_ext_v1_score < 560 then  '[540,560)'
     when 560 <= ccl_ext_v1_score and ccl_ext_v1_score < 580 then  '[560,580)'
     when 580 <= ccl_ext_v1_score and ccl_ext_v1_score < 600 then  '[580,600)'
     when 600 <= ccl_ext_v1_score and ccl_ext_v1_score < 620 then  '[600,620)'
     when 620 <= ccl_ext_v1_score and ccl_ext_v1_score < 640 then  '[620,640)'
     when 640 <= ccl_ext_v1_score and ccl_ext_v1_score < 660 then  '[640,660)'
     when 660 <= ccl_ext_v1_score and ccl_ext_v1_score < 680 then  '[660,680)'
     when 680 <= ccl_ext_v1_score and ccl_ext_v1_score < 700 then  '[680,700)'
     when 700 <= ccl_ext_v1_score and ccl_ext_v1_score < 720 then  '[700,720)'
     when ccl_ext_v1_score >= 720 then '720以上'
     end as ccl_ext_v1_score
from tmp.score_union_all
where ccl_ext_v1_score is not null
)t
group by loankind, period, ccl_ext_v1_score
)m
left join
(select split_part(loankind, "/", 2)as loankind, substr(loan_date,1,7)as period, count(*) as total
from tmp.score_union_all
where ccl_ext_v1_score is not null
group by loankind, period
)n
on m.loankind = n.loankind and m.period = n.period
order by m.loankind, m.period, ccl_ext_v1_score
;


# 聚信立分

select m.loankind, m.period, jxl_v1_score, cum, total, cum/total as dis
from 
(
select loankind, period, jxl_v1_score, count(*) as cum
from
(select split_part(loankind, "/", 2)as loankind, substr(loan_date,1,7)as period, 
case when jxl_v1_score <260 then '260以下'
     when 260 <= jxl_v1_score and jxl_v1_score < 280 then  '[260,280)'
     when 280 <= jxl_v1_score and jxl_v1_score < 300 then  '[280,300)'
     when 300 <= jxl_v1_score and jxl_v1_score < 320 then  '[300,320)'
     when 320 <= jxl_v1_score and jxl_v1_score < 340 then  '[320,340)'
     when 340 <= jxl_v1_score and jxl_v1_score < 360 then  '[340,360)'
     when 360 <= jxl_v1_score and jxl_v1_score < 380 then  '[360,380)'
     when 380 <= jxl_v1_score and jxl_v1_score < 400 then  '[380,400)'
     when 400 <= jxl_v1_score and jxl_v1_score < 420 then  '[400,420)'
     when 420 <= jxl_v1_score and jxl_v1_score < 440 then  '[420,440)'
     when 440 <= jxl_v1_score and jxl_v1_score < 460 then  '[440,460)'
     when 460 <= jxl_v1_score and jxl_v1_score < 480 then  '[460,480)'
     when 480 <= jxl_v1_score and jxl_v1_score < 500 then  '[480,500)'
     when 500 <= jxl_v1_score and jxl_v1_score < 520 then  '[500,520)'
     when 520 <= jxl_v1_score and jxl_v1_score < 540 then  '[520,540)'
     when 540 <= jxl_v1_score and jxl_v1_score < 560 then  '[540,560)'
     when 560 <= jxl_v1_score and jxl_v1_score < 580 then  '[560,580)'
     when 580 <= jxl_v1_score and jxl_v1_score < 600 then  '[580,600)'
     when 600 <= jxl_v1_score and jxl_v1_score < 620 then  '[600,620)'
     when 620 <= jxl_v1_score and jxl_v1_score < 640 then  '[620,640)'
     when 640 <= jxl_v1_score and jxl_v1_score < 660 then  '[640,660)'
     when 660 <= jxl_v1_score and jxl_v1_score < 680 then  '[660,680)'
     when 680 <= jxl_v1_score and jxl_v1_score < 700 then  '[680,700)'
     when 700 <= jxl_v1_score and jxl_v1_score < 720 then  '[700,720)'
     when jxl_v1_score >= 720 then '720以上'
     end as jxl_v1_score
from tmp.score_union_all
where jxl_v1_score is not null
)t
group by loankind, period, jxl_v1_score
)m
left join
(select split_part(loankind, "/", 2)as loankind, substr(loan_date,1,7)as period, count(*) as total
from tmp.score_union_all
where jxl_v1_score is not null
group by loankind, period
)n
on m.loankind = n.loankind and m.period = n.period
order by m.loankind, m.period, jxl_v1_score
;

# 豆豆手机分

select m.loankind, m.period, ddq_mob_as_v0925_score, cum, total, cum/total as dis
from 
(
select loankind, period, ddq_mob_as_v0925_score, count(*) as cum
from
(select split_part(loankind, "/", 2)as loankind, substr(loan_date,1,7)as period, 
case when ddq_mob_as_v0925_score <260 then '260以下'
     when 260 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 280 then  '[260,280)'
     when 280 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 300 then  '[280,300)'
     when 300 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 320 then  '[300,320)'
     when 320 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 340 then  '[320,340)'
     when 340 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 360 then  '[340,360)'
     when 360 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 380 then  '[360,380)'
     when 380 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 400 then  '[380,400)'
     when 400 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 420 then  '[400,420)'
     when 420 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 440 then  '[420,440)'
     when 440 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 460 then  '[440,460)'
     when 460 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 480 then  '[460,480)'
     when 480 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 500 then  '[480,500)'
     when 500 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 520 then  '[500,520)'
     when 520 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 540 then  '[520,540)'
     when 540 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 560 then  '[540,560)'
     when 560 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 580 then  '[560,580)'
     when 580 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 600 then  '[580,600)'
     when 600 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 620 then  '[600,620)'
     when 620 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 640 then  '[620,640)'
     when 640 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 660 then  '[640,660)'
     when 660 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 680 then  '[660,680)'
     when 680 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 700 then  '[680,700)'
     when 700 <= ddq_mob_as_v0925_score and ddq_mob_as_v0925_score < 720 then  '[700,720)'
     when ddq_mob_as_v0925_score >= 720 then '720以上'
     end as ddq_mob_as_v0925_score
from tmp.score_union_all
where ddq_mob_as_v0925_score is not null
)t
group by loankind, period, ddq_mob_as_v0925_score
)m
left join
(select split_part(loankind, "/", 2)as loankind, substr(loan_date,1,7)as period, count(*) as total
from tmp.score_union_all
where ddq_mob_as_v0925_score is not null
group by loankind, period
)n
on m.loankind = n.loankind and m.period = n.period
order by m.loankind, m.period, ddq_mob_as_v0925_score
;

# 卡卡手机分

select m.loankind, m.period, kkd_mob_as_v0925_score, cum, total, cum/total as dis
from 
(
select loankind, period, kkd_mob_as_v0925_score, count(*) as cum
from
(select split_part(loankind, "/", 2)as loankind, substr(loan_date,1,7)as period, 
case when kkd_mob_as_v0925_score <260 then '260以下'
     when 260 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 280 then  '[260,280)'
     when 280 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 300 then  '[280,300)'
     when 300 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 320 then  '[300,320)'
     when 320 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 340 then  '[320,340)'
     when 340 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 360 then  '[340,360)'
     when 360 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 380 then  '[360,380)'
     when 380 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 400 then  '[380,400)'
     when 400 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 420 then  '[400,420)'
     when 420 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 440 then  '[420,440)'
     when 440 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 460 then  '[440,460)'
     when 460 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 480 then  '[460,480)'
     when 480 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 500 then  '[480,500)'
     when 500 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 520 then  '[500,520)'
     when 520 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 540 then  '[520,540)'
     when 540 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 560 then  '[540,560)'
     when 560 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 580 then  '[560,580)'
     when 580 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 600 then  '[580,600)'
     when 600 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 620 then  '[600,620)'
     when 620 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 640 then  '[620,640)'
     when 640 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 660 then  '[640,660)'
     when 660 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 680 then  '[660,680)'
     when 680 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 700 then  '[680,700)'
     when 700 <= kkd_mob_as_v0925_score and kkd_mob_as_v0925_score < 720 then  '[700,720)'
     when kkd_mob_as_v0925_score >= 720 then '720以上'
     end as kkd_mob_as_v0925_score
from tmp.score_union_all
where kkd_mob_as_v0925_score is not null
)t
group by loankind, period, kkd_mob_as_v0925_score
)m
left join
(select split_part(loankind, "/", 2)as loankind, substr(loan_date,1,7)as period, count(*) as total
from tmp.score_union_all
where kkd_mob_as_v0925_score is not null
group by loankind, period
)n
on m.loankind = n.loankind and m.period = n.period
order by m.loankind, m.period, kkd_mob_as_v0925_score
;













