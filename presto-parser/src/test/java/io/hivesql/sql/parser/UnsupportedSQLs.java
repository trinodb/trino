package io.hivesql.sql.parser;

import io.prestosql.sql.parser.ParsingException;
import org.testng.annotations.Test;

public class UnsupportedSQLs extends SQLTester {

    @Test(expectedExceptions = ParsingException.class)
    public void sortByShouldThrowException()
    {
        String sql = "SELECT a from b sort by c";

        runHiveSQL(sql);
    }
}
