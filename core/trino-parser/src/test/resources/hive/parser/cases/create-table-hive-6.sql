create table if not exists tmp.tangyun_test
( site string, url string, pv bigint, label string)
row format delimited fields terminated by '\t'
stored as textfile