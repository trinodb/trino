package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class SetOperations extends SQLTester {

    @Test
    public void testUnion()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "UNION\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testUnionAll()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "UNION ALL\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testUnionDistinct()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "UNION DISTINCT\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testExcept()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "EXCEPT\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testExceptAll()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "EXCEPT ALL\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testExceptDistinct()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "EXCEPT DISTINCT\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testIntersect()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "INTERSECT\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testIntersectAll()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "INTERSECT ALL\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testIntersectDistinct()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "INTERSECT DISTINCT\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }
}
