 CREATE TABLE tmp.rec_like_event_orc (
    uid bigint,
    day string ,
    hour string
 )                                           
 WITH (                                      
    partitioned_by = ARRAY['day','hour'],
    format = 'ORC',
    external_location = '/tmp.db/rec_like_event_orc'
 )