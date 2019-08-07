package io.hivesql.sql.parser;

import org.testng.annotations.Test;

public class CasesFromFile extends SQLTester {

    @Test
    public void testCase01() {
        runHiveSQLFromFile("hive/parser/cases/case1.sql");
    }

    @Test
    public void testCase02() {
        checkASTNodeFromFile("hive/parser/cases/lateral-presto-1.sql",
                "hive/parser/cases/lateral-hive-1.sql");
    }
    @Test
    public void testCase03() {
        checkASTNodeFromFile("hive/parser/cases/lateral-presto-2.sql",
                "hive/parser/cases/lateral-hive-2.sql");
    }
}
