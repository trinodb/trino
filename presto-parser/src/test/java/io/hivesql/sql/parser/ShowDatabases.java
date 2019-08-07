package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class ShowDatabases extends SQLTester {

    @Test
    public void testShowDatabases()
    {
        String hiveSql = "SHOW DATABASES";
        String prestoSql = "SHOW SCHEMAS";
        checkASTNode(prestoSql, hiveSql);
    }
    @Test
    public void testShowDatabasesWithLike()
    {
        String hiveSql = "SHOW DATABASES like '%tmp%'";
        String prestoSql = "SHOW SCHEMAS  like '%tmp%'";
        checkASTNode(prestoSql, hiveSql);
    }

}
