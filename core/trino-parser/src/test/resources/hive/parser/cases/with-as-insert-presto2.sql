insert overwrite tmp.test
with
t1 as (select uid,name,day from tmp.test),
t2 as (select uid,name,day from tmp.test),
t3 as (select uid,name,day from tmp.test)
select uid,name,'2019-11-11' day from t1
union all
select uid,name,'2019-11-11' day from t2
union all
select uid,name,'2019-11-11' day from t3