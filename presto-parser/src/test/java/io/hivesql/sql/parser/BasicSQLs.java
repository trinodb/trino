package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class BasicSQLs extends SQLTester {
//    @Test
//    public void testUse()
//    {
//        String sql = "USE hive.tmp";
//        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
//        System.out.println(node);
//    }
//
//    @Test
//    public void testSetSession()
//    {
//        String sql = "SET SESSION foo=true";
//        Node node = sqlParser.createStatement(sql, hiveParsingOptions);
//        System.out.println(node);
//    }

    @Test
    public void testFuncCall()
    {
        String sql = "SELECT func1(a)";

        checkASTNode(sql);
    }

    @Test
    public void testFuncNestedCall()
    {
        String sql = "SELECT func1(func2(a))";

        checkASTNode(sql);
    }

    @Test
    public void testSelectExpression()
    {
        String sql = "SELECT 1 + 2 + 3";

        checkASTNode(sql);
    }

    @Test
    public void testColumnAlias()
    {
        String sql = "SELECT 1 as cnt";

        checkASTNode(sql);
    }

    @Test
    public void testSelectFrom()
    {
        String sql = "SELECT a from tb1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectAllFrom()
    {
        String sql = "SELECT a, * from tb1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectAllWithQualifierFrom()
    {
        String sql = "SELECT tb1.* from tb1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectFromTableAlias()
    {
        String sql = "SELECT t.a from tb1 t";

        checkASTNode(sql);
    }

    @Test
    public void testSelectFromWithDBAndTable()
    {
        String sql = "SELECT a from db1.tb1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectFromWithDistinct()
    {
        String sql = "SELECT distinct a from tb1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectFromWithBacktick()
    {
        String prestoSql = "SELECT a FROM tb1";
        String hiveSql = "SELECT `a` FROM tb1";

        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testWhereClause()
    {
        String sql = "SELECT a, b from tb1 where c > 10 and c <> 'hello' or (d = true and e is not null)";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseIsNotNull()
    {
        String sql = "SELECT a from tb1 where e is not null";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseIsNull()
    {
        String sql = "SELECT a from tb1 where e is null";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseBetween()
    {
        String sql = "SELECT a from tb1 where e BETWEEN start1 and end2";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseNotBetween()
    {
        String sql = "SELECT a from tb1 where e NOT BETWEEN start1 and end2";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseIn()
    {
        String sql = "SELECT a from tb1 where e in(1, 2)";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseNotIn()
    {
        String sql = "SELECT a from tb1 where e not in(1, 2)";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseLike()
    {
        String sql = "SELECT a from tb1 where e like 'a%'";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseNotLike()
    {
        String sql = "SELECT a from tb1 where e not like 'a%'";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseDistinct()
    {
        String sql = "SELECT a from tb1 where e is distinct from end_day";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseNotDistinct()
    {
        String sql = "SELECT a from tb1 where e is not distinct from end_day";

        checkASTNode(sql);
    }

    @Test
    public void testLimit()
    {
        String sql = "SELECT a from tb1 limit 10";

        checkASTNode(sql);
    }

    @Test
    public void testOrderBy()
    {
        String sql = "SELECT a from tb1 order by t desc";

        checkASTNode(sql);
    }
}
