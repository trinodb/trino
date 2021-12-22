SELECT day,
   concat(decode(unhex(hex(cast((k5 & 992) / 32 AS INT) + ascii('A'))), 'US-ASCII'),
   decode(unhex(hex(cast(k5 & 31 AS INT) + ascii('A'))), 'US-ASCII')) AS country,
   (players.k80 & 1015808)/32768 AS stuck_times_200ms_0,
   (players.k80 & 32505856)/1048576 AS stuck_times_200ms_1,
   (players.k80 & 1040187392)/33554432 AS stuck_times_200ms_2
FROM bigolive.live_sdk_video_stats_event_simplification
cross join unnest(k11) as players (k79,k80,k81,k82,k83,k84,k85,k86,k87,k88,k89,k90,k91,k92,k93,k94,k95,k96,k97,k98,k99,k100,k101,k102,k103,k104,k105,k106,k107)
WHERE
   day between '2019-07-26' and '2019-08-01'
   AND players IS NOT NULL
   AND players.k80 <> 4294967295
   AND (k35 = 4294967295 OR k35%60 = 0 OR k35%60 >= 5)