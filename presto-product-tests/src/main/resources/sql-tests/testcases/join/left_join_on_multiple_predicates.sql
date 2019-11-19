-- database: presto; groups: join; tables: nation, part
select n_name, r_name from nation left outer join region on n_regionkey = r_regionkey and n_name = r_name
