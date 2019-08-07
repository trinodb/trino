package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class Aggregations extends SQLTester {

    @Test
    public void testGroupBy()
    {
        String sql = "SELECT a, b, count(1) as cnt from tb1 group by a, b";

        checkASTNode(sql);
    }

    @Test
    public void testGroupByCube()
    {
        String prestoSql = "SELECT a, b, count(1) as cnt from tb1 group by CUBE(a, b)";
        String hiveSql = "SELECT a, b, count(1) as cnt from tb1 group by a, b with CUBE";

        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testGroupByRollup()
    {
        String prestoSql = "SELECT a, b, count(1) as cnt from tb1 group by ROLLUP(a, b)";
        String hiveSql = "SELECT a, b, count(1) as cnt from tb1 group by a, b with ROLLUP";

        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testGroupByGroupingSets()
    {
        String prestoSql = "SELECT a, b, c, count(1) as cnt from tb1 group by GROUPING SETS ( (a, b, c), (a, b), (b, c), (a, c), (a), (b), (c), ( ))";
        String hiveSql = "SELECT a, b, c, count(1) as cnt from tb1 GROUP BY a, b, c GROUPING SETS ( (a, b, c), (a, b), (b, c), (a, c), (a), (b), (c), ( ))";

        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testHaving()
    {
        String sql = "SELECT a from tb1 group by a having COUNT(b) > 25";

        checkASTNode(sql);
    }
}
