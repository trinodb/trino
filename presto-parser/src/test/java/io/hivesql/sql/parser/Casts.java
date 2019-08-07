package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class Casts extends SQLTester {

    @Test
    public void testCast()
    {
        String sql = "SELECT cast ('1' as INT)";

        checkASTNode(sql);
    }

    @Test
    public void testCastArray()
    {
        String sql = " SELECT CAST(1 AS ARRAY<INT>)";

        checkASTNode(sql);
    }

    @Test
    public void testCastMap()
    {
        String sql = "SELECT cast ('1' as MAP<INT,BIGINT>)";

        checkASTNode(sql);
    }

    @Test
    public void testCastStruct()
    {
        String prestoSql = "SELECT CAST(1 AS ROW(x BIGINT, y VARCHAR))";
        String hiveSql = "SELECT CAST(1 AS STRUCT<x: BIGINT, y: VARCHAR>)";

        checkASTNode(prestoSql, hiveSql);
    }
}
