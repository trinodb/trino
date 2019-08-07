SELECT
'${day}' as sday,
coalesce(event.event_info['type'], 100) as list_type,
coalesce(os, 'all') as os,
coalesce(client_version, 'all') as client_version,
coalesce(country, 'all') as country,
coalesce(event.event_info['net'], 'all') as net,
coalesce(if(event.event_info['is_first_pull'] in('true', '1'), 1, 0), 100) as is_first_pull,
sum(if(event.event_info['action'] in(2, 3), 1, 0)) as res_cnt,
sum(if(event.event_info['action']==2 AND event.event_info['res_code'] in(0, 200), 1, 0)) as suc_res_cnt,
coalesce(sum(if(event.event_info['action']==2 AND event.event_info['res_code'] in(0, 200), 1, 0))/sum(if(event.event_info['action'] in(2, 3), 1, 0)), 0) as suc_rate,
coalesce(sum(if(event.event_info['action']==2 AND event.event_info['res_code'] in(0, 200),
        if(event.event_info['linkd_connected_ts'] < 0
         OR event.event_info['action_ts'] - event.event_info['linkd_connected_ts'] < 0
         OR event.event_info['action_ts'] - event.event_info['linkd_connected_ts'] > event.event_info['cost'],
        event.event_info['cost'],
        event.event_info['action_ts'] - event.event_info['linkd_connected_ts']), 0)
    ) / sum(if(event.event_info['action']==2 AND event.event_info['res_code'] in(0, 200), 1, 0)), 0) as avg_suc_cost,
avg(if(event.event_info['action']==2
       AND event.event_info['res_code'] IN(0, 200), if(event.event_info['linkd_connected_ts'] < 0
                                                       OR event.event_info['action_ts'] - event.event_info['linkd_connected_ts'] < 0
                                                       OR event.event_info['action_ts'] - event.event_info['linkd_connected_ts'] > if(event.event_info['partial_show'] > 0, event.event_info['partial_show'], event.event_info['cost']),
                                                       if(event.event_info['partial_show'] > 0, event.event_info['partial_show'], event.event_info['cost']), event.event_info['action_ts'] - event.event_info['linkd_connected_ts']), NULL)) AS avg_partial_show,

percentile_approx(if(event.event_info['action']==2
       AND event.event_info['res_code'] IN(0, 200), cast(if(event.event_info['linkd_connected_ts'] < 0
                                                       OR event.event_info['action_ts'] - event.event_info['linkd_connected_ts'] < 0
                                                       OR event.event_info['action_ts'] - event.event_info['linkd_connected_ts'] > if(event.event_info['partial_show'] > 0, event.event_info['partial_show'], event.event_info['cost']),
                                                       if(event.event_info['partial_show'] > 0, event.event_info['partial_show'], event.event_info['cost']), event.event_info['action_ts'] - event.event_info['linkd_connected_ts']) as bigint), NULL), 0.9) AS 90_partial_show,
coalesce(sum(if(event.event_info['action']=6 AND event.event_info['is_first_pull'] in('true', '1') AND event.event_info['cost']>0 AND event.event_info['cost']<= 30000, event.event_info['cost'], 0))
    / sum(if(event.event_info['action']=6 AND event.event_info['is_first_pull'] in('true', '1') AND event.event_info['cost']>0 AND event.event_info['cost']<= 30000 , 1, 0)), 0) as avg_user_first_cost,
percentile_approx((if(event.event_info['action']==2 AND event.event_info['res_code'] in(0, 200),
        cast(if(event.event_info['linkd_connected_ts'] < 0
        OR event.event_info['action_ts'] < event.event_info['linkd_connected_ts']
        OR event.event_info['action_ts'] - event.event_info['linkd_connected_ts'] > event.event_info['cost'],
        event.event_info['cost'],
        event.event_info['action_ts'] - event.event_info['linkd_connected_ts']) as bigint), NULL)),0.9) as 90_percentile_cost
from vlog.like_user_event_hour_orc
WHERE event_id='0501016'
AND day='${day}'
AND event.event_info['type']=1
AND event.event_info['linkd']=2
AND country regexp '^[a-zA-Z]{2}$'
AND client_version not in (1271,1283)
group by os, client_version, country,
if(event.event_info['is_first_pull'] in('true', '1'), 1, 0),
event.event_info['net'],
event.event_info['type']
with cube;