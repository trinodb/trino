package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class SubQueries extends SQLTester {
    @Test
    public void testInSubQuery()
    {
        String sql = "" +
                "SELECT ProductName\n" +
                "  FROM Product \n" +
                "WHERE Id IN (SELECT ProductId \n" +
                "                FROM OrderItem\n" +
                "               WHERE Quantity > 100)\n";

        checkASTNode(sql);
    }

    @Test
    public void testExistsSubQuery()
    {
        String sql = "" +
                "SELECT ProductName\n" +
                "  FROM Product \n" +
                "WHERE EXISTS (SELECT ProductId \n" +
                "                FROM OrderItem\n" +
                "               WHERE Quantity > 100)\n";

        checkASTNode(sql);
    }

    @Test
    public void testTableSubQuery()
    {
        String sql = "" +
                "SELECT ProductName\n" +
                "FROM (\n" +
                "    SELECT ProductName \n" +
                "    FROM OrderItem\n" +
                "    WHERE Quantity > 100\n" +
                ") t1";

        checkASTNode(sql);
    }
}
