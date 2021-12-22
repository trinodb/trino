insert overwrite tmp.test
with
t1 as (select uid,name,day from tmp.test)
select uid,name,'2019-11-11' day from t1