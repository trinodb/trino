select day, show_uv_all*1.00/daily_service_user as show_rate
from
(
  select day as day1,show_uv_all
  from tmp.push_show_type_8_show_click
  where day between '2019-07-01' and '2019-08-28'
  and country = 'in'
)t1
cross join
(
  select day, flag1_cnt as daily_service_user
  from tmp.tbl
  where day between '2019-07-01' and '2019-08-28'
  and country = 'in'
)t2
where t1.day1 = t2.day
