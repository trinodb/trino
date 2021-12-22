select i.uids, u from bigolive.bigo_online_user_uversion_stat_platform
cross join unnest(online) as i
cross join unnest(i.uids) as uids (u)
where day='2019-08-08'
limit 10