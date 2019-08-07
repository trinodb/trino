package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class ConditionalFunctions extends SQLTester {

    @Test
    public void testIf()
    {
        String sql = "select IF(1=1,'TRUE','FALSE')";

        checkASTNode(sql);
    }

    @Test
    public void testNullIf()
    {
        String sql = "select nullif(a, b)";

        checkASTNode(sql);
    }

    @Test
    public void testCoalesce()
    {
        String sql = "select coalesce(a, b, c)";

        checkASTNode(sql);
    }

    @Test
    public void testCaseWhen()
    {
        String sql = "" +
                "SELECT\n" +
                "CASE   Fruit\n" +
                "       WHEN 'APPLE' THEN 'The owner is APPLE'\n" +
                "       WHEN 'ORANGE' THEN 'The owner is ORANGE'\n" +
                "       ELSE 'It is another Fruit'\n" +
                "END";

        checkASTNode(sql);
    }
}
