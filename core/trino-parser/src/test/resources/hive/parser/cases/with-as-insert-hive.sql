with
t1 as (select uid,name,day from tmp.test)
insert into tmp.test
select uid,name,day from t1
