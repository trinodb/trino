select i.uids, u from bigolive.bigo_online_user_uversion_stat_platform
lateral view explode(online) o as i
lateral view explode(i.uids) uids as u
where day='2019-08-08'
limit 10