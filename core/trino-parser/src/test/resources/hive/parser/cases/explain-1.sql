explain select
    m.groupid,
    rtime,
    groupnums
from
(
    select groupid,uid,groupnums,rtime
    from indigo_us.monitormanager_open_groupchat_beta
    where day>='2019-08-01'
    and groupid='bg.cjsfa5dxibi92wyr'
    and grouptype=1
    union all
    select groupid,uid,groupnums,rtime
    from indigo_us.monitormanager_open_groupchat_stable
    where day>='2019-08-01'
    and groupid='bg.cjsfa5dxibi92wyr'
    and grouptype=1
    union all
    select groupid,uid,groupnums,rtime
    from indigo_us.monitormanager_open_groupchat_ios
    where day>='2019-08-01'
    and groupid='bg.cjsfa5dxibi92wyr'
    and grouptype=1
)m
order by m.rtime
