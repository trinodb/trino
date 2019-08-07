package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class Literals extends SQLTester {

    @Test
    public void testSelectIntegerLiteral()
    {
        String sql = "SELECT 1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectDoubleLiteral()
    {
        String sql = "SELECT 1.5";

        checkASTNode(sql);
    }

    @Test
    public void testSelectIntegerValueDoubleLiteral()
    {
        String sql = "SELECT 1.0";

        checkASTNode(sql);
    }

    @Test
    public void testSelectStringLiteral()
    {
        String sql = "SELECT 'abc'";

        checkASTNode(sql);
    }

    @Test
    public void testSelectStringLiteralWithDoubleQuotation()
    {
        String prestoSql = "SELECT 'abc'";
        String hiveSql = "SELECT \"abc\"";

        checkASTNode(prestoSql, hiveSql);
    }
}
