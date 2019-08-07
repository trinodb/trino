package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class LateralView extends SQLTester {
    @Test
    public void testLateralView()
    {
        String prestoSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 CROSS JOIN UNNEST(events) AS event (c1)";
        String hiveSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 lateral view explode(events) event as c1";

        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testLateralViewWithOrdinality()
    {
        String prestoSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 CROSS JOIN UNNEST(events) WITH ORDINALITY AS event (pos, c1)";
        String hiveSql = "" +
                "SELECT event.*\n" +
                "FROM tb1 lateral view posexplode(events) event as pos, c1";

        checkASTNode(prestoSql, hiveSql);
    }
}
