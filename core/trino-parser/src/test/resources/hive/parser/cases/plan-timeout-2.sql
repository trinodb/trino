SELECT
event.event_info['type'] as type,
if(CAST(event.event_info['avgspeed'] AS FLOAT) <= 0, 'avgspeed_0', 'avgspeed_1') as avgspeed,
if(CAST(event.event_info['taskid'] AS INT) <= 0, 'taskid_0', 'taskid_1') as taskid,
if(CAST(event.event_info['timedown'] AS INT) <= 0, 'timedown_0', 'timedown_1') as timedown,
if(CAST(event.event_info['firstpkgtime'] AS INT) <= 0, 'firstpkgtime_0', 'firstpkgtime_1') as firstpkgtime,
if(CAST(event.event_info['firstrestime'] AS FLOAT) <= 0, 'firstrestime_0', 'firstrestime_1') as firstrestime,
count(1)
from indigo.indigo_show_user_event_orc
where event.event_id = '01000031'
  and day = '2019-08-07'
  and ( client_version >= '1968' and client_version <= '9999' )
  and client_version <> '1969'
  and event.event_info['errstage'] = '0'
  and event.event_info['downtype'] = '1'
  and event.event_info['reconnect_times'] = '0'
  and event.event_info['mode'] in ('quicEncrypt', 'tcp')
  and event.event_info['predownpercent'] <> '100'
  and strpos(event.event_info['errmsg'], '3001') = 0
  and CAST(event.event_info['filesize'] AS INT)>0
  group by
  event.event_info['type'],
if(CAST(event.event_info['avgspeed'] AS FLOAT) <= 0, 'avgspeed_0', 'avgspeed_1'),
if(CAST(event.event_info['taskid'] AS INT) <= 0, 'taskid_0', 'taskid_1'),
if(CAST(event.event_info['timedown'] AS INT) <= 0, 'timedown_0', 'timedown_1'),
if(CAST(event.event_info['firstpkgtime'] AS INT) <= 0, 'firstpkgtime_0', 'firstpkgtime_1'),
if(CAST(event.event_info['firstrestime'] AS FLOAT) <= 0, 'firstrestime_0', 'firstrestime_1') limit 200000