create table if not exists tmp.test (
  id string,
  sex string
)ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  ESCAPED BY 'm'