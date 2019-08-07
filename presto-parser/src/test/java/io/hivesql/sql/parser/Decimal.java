package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class Decimal extends SQLTester {

    @Test
    public void test01() {
        String sql = "select 1e2";
        checkASTNode(sql);
    }
    @Test
    public void test02() {
        String sql = "select 1.1e2";
        checkASTNode(sql);
    }
    @Test
    public void test03() {
        String hiveSql = "select 1.1e2D";
        String prestoSql = "select 1.1e2";
        checkASTNode(prestoSql, hiveSql);
    }
}
