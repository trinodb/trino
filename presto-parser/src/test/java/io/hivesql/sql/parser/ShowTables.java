package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class ShowTables extends SQLTester {

    @Test
    public void testShowDatabases()
    {
        String hiveSql = "SHOW TABLES FROM DB";
        String prestoSql = "SHOW TABLES FROM DB";
        checkASTNode(prestoSql, hiveSql);
    }
    @Test
    public void testShowDatabasesWithLike()
    {
        String hiveSql = "SHOW TABLES like '%tmp%'";
        String prestoSql = "SHOW TABLES  like '%tmp%'";
        checkASTNode(prestoSql, hiveSql);
    }

}
