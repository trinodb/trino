CREATE TABLE tmp.test
(
  uid bigint,
  items array(row(idx bigint, extra map(string, string), m int))
)

