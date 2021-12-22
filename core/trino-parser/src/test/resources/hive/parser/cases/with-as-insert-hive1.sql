with
t1 as (select uid,name,day from tmp.test)
insert overwrite table tmp.test partition(day='2019-11-11')
select uid,name from t1