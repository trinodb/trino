CREATE TABLE `tmp.test`(
  `uid` bigint, 
  `items` array<struct<idx:bigint,extra:map<string,string>, m:int>>
)
