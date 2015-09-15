===可以push的用户总表 userid,pushid,uid  挖财的userid uid 
create table tmp_wac.push_total_list as
select m.userid, m.pushid, n.uid
from
(select userid, pushid 
from 
default.s_tbl_user_push) m
join (select id, uid from default.tbl_member) n
on m.userid = n.id



===已经push过的用户 
drop table tmp_wac.pushed_list;
create table tmp_wac.pushed_list as 
select distinct version,id as uid,source from (
select 'v1_morning' as version,* from tmp_wac.getui_pushmorning 
union all
select 'v1_night' as version,* from tmp_wac.getui_pushnight
) tp;

----插入数据
==id = userid

insert into table tmp_wac.pushed_list
select distinct version,id,source from (
select userid as uid, source, 'v2_morning' as version from tmp_wac.getui_pushmorningv2

)tp


===去除tmp_wac.pushed_list中的；取出tmp_wac.push_total_list中有的
===样本框  tmp_wac.to_push_member        
-----userid,uid,pushid
drop table tmp_wac.to_push_member;
create table tmp_wac.to_push_member as
select a.userid,a.uid,a.pushid
from tmp_wac.push_total_list a left join tmp_wac.pushed_list b on a.userid=b.id
where b.id is null;




===
===uid = wcuid
---source=0, 社区活跃用户
create table tmp_wac.getui_sampleV3 as 

select bl.userid, bl.uid, bl.pushid,0 as source,
row_number()over () as rid
from 
(select pm.userid,pm.uid,pm.pushid
from
(select
distinct n.uid
from(
select 
uid,
count(distinct FROM_UNIXTIME(a.visit_time,'yyyy-MM-dd')) as come
from default.s_tbl_bbs_user_visit_thread_log a join default.s_forum_thread b on a.thread_id=b.tid 
where dt between '2015-07-20' and '2015-08-21' and uid<>0 and uid<>1
and  FROM_UNIXTIME(a.visit_time,'yyyy-MM-dd') between '2015-07-20' and '2015-08-20'
group by 
uid ) m  join 
(select
uid,
FROM_UNIXTIME(c.visit_time,'yyyy-MM-dd') as date
from default.s_tbl_bbs_user_visit_thread_log c join default.s_forum_thread d on c.thread_id=d.tid 
where dt between '2015-07-20' and '2015-08-21' 
and  FROM_UNIXTIME(c.visit_time,'yyyy-MM-dd') between '2015-07-20' and '2015-08-20' ) n on m.uid=n.uid
where come>=4 ) chy 
join default.s_common_member tp1 on chy.uid=tp1.uid
join tmp_wac.to_push_member pm on tp1.wcuid=pm.uid
distribute by rand()
sort by rand()
limit 4000) bl

union all
---source=1, 社区非活跃用户
select bl.userid, bl.uid, bl.pushid,1 as source,
row_number()over () as rid
from
(select pm.userid,pm.uid,pm.pushid
from
(select 
m.uid
from 
(select 
distinct uid 
from default.s_tbl_bbs_user_visit_thread_log a join default.s_forum_thread b on a.thread_id=b.tid 
where dt between '2015-06-20' and '2015-08-06' and  uid<>0 and uid<>1 and uid<>2
and  FROM_UNIXTIME(visit_time,'yyyy-MM-dd') between '2015-06-20' and '2015-08-04') m 
left join 
(select 
distinct uid
from default.s_tbl_bbs_user_visit_thread_log c join default.s_forum_thread d on c.thread_id=d.tid 
where dt between '2015-08-04' and '2015-08-21' and  uid<>0 and uid<>1 and uid<>2
and  FROM_UNIXTIME(visit_time,'yyyy-MM-dd') between '2015-08-05' and '2015-08-20') n on m.uid=n.uid
where n.uid is null) cfhy 
join default.s_common_member tp1 on cfhy.uid=tp1.uid
join tmp_wac.to_push_member pm on tp1.wcuid=pm.uid
distribute by rand()
sort by rand()
limit 20000) bl

union all
---source=2, 记账用户，通过莲子的log表查找近期有过记账行为的用户
select bl.userid, bl.uid, bl.pushid,2 as source,
row_number()over () as rid
from
(select pm.userid,pm.uid,pm.pushid
from
(select m.wcuid 
from 
(select distinct a.userid,
b.uid as wcuid
from default.lianzi_log_event a join default.tbl_member b on a.userid=b.id
where dt>='2015-07-20' 
and to_date(event_date) between '2015-07-20' and '2015-08-20'
and b.uid is not null) m
left join default.s_common_member n on m.wcuid=n.wcuid
where n.wcuid is null ) jzapp
join tmp_wac.to_push_member pm on jzapp.wcuid=pm.uid
distribute by rand()
sort by rand()
limit 62000) bl

union all
---source=3, 理财用户，通过p2p_order的近期订单筛选出来的用户
select bl.userid, bl.uid, bl.pushid,3 as source,
row_number()over () as rid
from
(select pm.userid,pm.uid,pm.pushid
from
(select distinct a.uid 
from default.s_p2p_order a
left join default.s_common_member b on a.uid=b.wcuid
where to_date(ordertime ) between '2015-05-20' and '2015-08-20'
and b.wcuid is null) lcapp
join tmp_wac.to_push_member pm on lcapp.uid=pm.uid
distribute by rand()
sort by rand()
limit 4000) bl

union all
---source=4, 快贷用户，小刀的逻辑
select bl.userid, bl.uid, bl.pushid,4 as source,
row_number()over () as rid
from
(select pm.userid,pm.uid,pm.pushid
from
(select kd.uid from (
select distinct d.uid 
from
(
    select app_id_ ,to_date(create_) applydate from default.s_wf_hist_task
    where result_='pass'
    and description_='资料初审' 
    and state_='completed' 
) a
left join
(
    select distinct app_id_ from 
    (
        select app_id_ ,assignee_ from default.s_wf_task
        union all
        select app_id_,assignee_ from default.s_wf_hist_task
    ) a 
    where assignee_='B0267'
) c
on a.app_id_=c.app_id_
join
(
    select app_no,wc_uid uid,wc_app_no,id_no,actual_amt,
    case
    when enter_type in ('01','2','3','6','7') then '记账'
    when enter_type in ('30','31','33') then '钱管家'
    when enter_type in ('60','61') then '快贷'
    else '其他' end source
    from default.s_iqp_project
) d
on a.app_id_=d.app_no
where c.app_id_ is null

union all

select distinct a.uid 
from
(
    select distinct id,applytime,idno,loanAmout,uid
    from default.s_loan_consumer_application
    where loantype='6'
) a
join
(
    select distinct aid from default.s_loan_material_references where valid=1 and type=10
) c
on a.id=c.aid ) kd left join default.s_common_member tp3 on kd.uid=tp3.wcuid
where tp3.wcuid is null )kdapp
join tmp_wac.to_push_member pm on kdapp.uid=pm.uid
distribute by rand()
sort by rand()
limit 10000) bl