select
CAST(json_parse(o1) AS MAP(INTEGER,ROW(c1 ARRAY(STRING), c2 INTEGER)))
from tbl
