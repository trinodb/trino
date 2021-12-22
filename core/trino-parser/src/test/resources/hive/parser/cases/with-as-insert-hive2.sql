with
t1 as (select uid,name,day from tmp.test),
t2 as (select uid,name,day from tmp.test),
t3 as (select uid,name,day from tmp.test)
insert overwrite table tmp.test partition(day='2019-11-11')
select uid,name from t1
union all
select uid,name from t2
union all
select uid,name from t3
