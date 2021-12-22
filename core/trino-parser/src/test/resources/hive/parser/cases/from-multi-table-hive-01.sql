select
t1.success_num as success_num,
t2.error_num as error_num,
t1.success_num / (t1.success_num  + t2.error_num ) as rate,
t3.unknown_num
from
(
    select
    count(*) success_num
    from bigolive.bigo_show_user_event_hour_orc
    where event_id='011410004'
    and day = '2019-07-25'
    and (os = 'Android' or os = 'iOS')
) as t1,
(
    select
    count(*) error_num
    from bigolive.bigo_show_user_event_hour_orc
    where event_id="011410005"
    and day = '2019-07-25'
    and (os = 'Android' or os = 'iOS')
) as t2,
(
    select
    count(*) unknown_num
    from bigolive.bigo_show_user_event_hour_orc
    where event_id='011410006'
    and day = '2019-07-25'
    and (os = 'Android' or os = 'iOS')
) as t3