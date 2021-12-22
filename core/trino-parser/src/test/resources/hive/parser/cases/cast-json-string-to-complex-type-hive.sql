select from_json(o1, 'MAP<INTEGER,STRUCT<c1:ARRAY<STRING>,c2:INTEGER>>')
from tbl
