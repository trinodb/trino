/**
 * Unpublished work.
 * Copyright 2025 by Teradata Corporation. All rights reserved
 * TERADATA CORPORATION CONFIDENTIAL AND TRADE SECRET
 */

package io.trino.plugin.teradata;

import org.junit.jupiter.api.Test;

public class TestTeradataJDBCQueries
        extends AbstractTeradataJDBCTest
{
    public TestTeradataJDBCQueries()
    {
        super("trino_test_query");
    }

    @Override
    protected void initTables()
    {
        // --------
        // inttable
        // --------
        database.execute(String.format("""
                create table %s.inttable
                (test varchar(16)
                ,col1_byteint byteint
                ,col2_smallint smallint
                ,col3_int int
                ,col4_bigint bigint
                ) no primary index;
                ;""", databaseName));
        database.execute(String.format("insert into %s.inttable values ('null',null,null,null,null);", databaseName));
        database.execute(String.format("insert into %s.inttable values ('negative',-128,-32768,-2147483648,-9223372036854775808);", databaseName));
        database.execute(String.format("insert into %s.inttable values ('positive',127,32767,2147483647,9223372036854775807);", databaseName));
        // --------
        // chartable
        // --------
        database.execute(String.format("""
                create table %s.chartable
                (test varchar(16)
                ,col1_latinchar1 char(1) character set latin not casespecific
                ,col2_latinchar8 char(8) character set latin not casespecific
                ,col3_cschar1 char(1) character set latin casespecific
                ,col4_cschar8 char(8) character set latin casespecific
                ,col5_unicodechar1 char(1) character set unicode not casespecific
                ,col6_unicodechar8 char(8) character set unicode not casespecific
                ,col7_csunicodechar1 char(1) character set unicode casespecific
                ,col8_csunicodechar8 char(8) character set unicode casespecific
                ) no primary index;
                ;""", databaseName));
        database.execute(String.format("insert into %s.chartable values ('null',null,null,null,null,null,null,null,null);", databaseName));
        database.execute(String.format("insert into %s.chartable values ('simple','a','abcdefgh','A','AbCdEfGh','a','AbCdEfGh','A','AbCdEfGh');", databaseName));
        database.execute(String.format("insert into %s.chartable values ('unicode','a','abcdefgh','A','AbCdEfGh','α','ΑβΓδΕζΗθ','Α','ΑβΓδΕζΗθ');", databaseName));
    }

    @Test
    public void selectFromIntTable()
    {
        assertQuery(String.format("SELECT * FROM teradata.%s.inttable where test = 'null'", databaseName), "VALUES ('null', null, null, null, null)");
        assertQuery(String.format("SELECT * FROM teradata.%s.inttable where test = 'negative'", databaseName), "VALUES ('negative', -128, -32768, -2147483648, -9223372036854775808)");
        assertQuery(String.format("SELECT * FROM teradata.%s.inttable where test = 'positive'", databaseName), "VALUES ('positive', 127, 32767, 2147483647, 9223372036854775807)");
    }

    @Test
    public void selectLatinFromCharTable()
    {
        assertQuery(String.format("SELECT * FROM teradata.%s.chartable where test = 'null'", databaseName), "VALUES ('null', null, null, null, null, null, null, null, null)");
        assertQuery(String.format("SELECT * FROM teradata.%s.chartable where test = 'simple'", databaseName), "VALUES ('simple', 'a', 'abcdefgh', 'A', 'AbCdEfGh', 'a', 'AbCdEfGh', 'A', 'AbCdEfGh')");
    }

    @Test
    public void selectUnicodeFromCharTable()
    {
        // this will fail unless the teradata charset is unicode (e.g. charset=utf8)
        assertQuery(String.format("SELECT * FROM teradata.%s.chartable where test = 'unicode'", databaseName), "VALUES ('unicode', 'a', 'abcdefgh', 'A', 'AbCdEfGh', 'α','ΑβΓδΕζΗθ','Α','ΑβΓδΕζΗθ')");
    }

    @Test
    public void selectLatinNotCaseSpecificWhere()
    {
        assertQuery(String.format("SELECT * FROM teradata.%s.chartable where test = 'simple' and col1_latinchar1 = 'a'", databaseName), "VALUES ('simple', 'a', 'abcdefgh', 'A', 'AbCdEfGh', 'a', 'AbCdEfGh', 'A', 'AbCdEfGh')");
        assertQuery(String.format("SELECT * FROM teradata.%s.chartable where test = 'SIMPLE' and col1_latinchar1 = 'A'", databaseName), "VALUES ('simple', 'a', 'abcdefgh', 'A', 'AbCdEfGh', 'a', 'AbCdEfGh', 'A', 'AbCdEfGh')");
    }

    @Test
    public void selectUnicodeNotCaseSpecificWhere()
    {
        assertQuery(String.format("SELECT * FROM teradata.%s.chartable where test = 'unicode' and col5_unicodechar1 = 'α'", databaseName), "VALUES ('unicode', 'a', 'abcdefgh', 'A', 'AbCdEfGh', 'α','ΑβΓδΕζΗθ','Α','ΑβΓδΕζΗθ')");
        assertQuery(String.format("SELECT * FROM teradata.%s.chartable where test = 'UNICODE' and col5_unicodechar1 = 'Α'", databaseName), "VALUES ('unicode', 'a', 'abcdefgh', 'A', 'AbCdEfGh', 'α','ΑβΓδΕζΗθ','Α','ΑβΓδΕζΗθ')");
    }

    @Test
    public void selectLatinCaseSpecificWhere()
    {
        assertQuery(String.format("SELECT * FROM teradata.%s.chartable where test = 'simple' and col3_cschar1 = 'A'", databaseName), "VALUES ('simple', 'a', 'abcdefgh', 'A', 'AbCdEfGh', 'a', 'AbCdEfGh', 'A', 'AbCdEfGh')");
        assertions.assertQueryReturnsEmptyResult(String.format("SELECT test FROM teradata.%s.chartable where test = 'SIMPLE' and col3_cschar1 = 'a'", databaseName));
    }

    @Test
    public void selectUnicodeCaseSpecificWhere()
    {
        assertQuery(String.format("SELECT * FROM teradata.%s.chartable where test = 'unicode' and col7_csunicodechar1 = 'Α'", databaseName), "VALUES ('unicode', 'a', 'abcdefgh', 'A', 'AbCdEfGh', 'α','ΑβΓδΕζΗθ','Α','ΑβΓδΕζΗθ')");
        assertions.assertQueryReturnsEmptyResult(String.format("SELECT test FROM teradata.%s.chartable where test = 'UNICODE' and col7_csunicodechar1 = 'α'", databaseName));
    }

    @Test
    public void selectColumnCaseFromTable()
    {
        // single column - case match
        assertQuery(String.format("SELECT test FROM teradata.%s.inttable where test = 'null'", databaseName), "VALUES ('null')");
        assertQuery(String.format("SELECT col1_byteint FROM teradata.%s.inttable where test = 'null'", databaseName), "VALUES (null)");
        // single column - case mismatch
        assertQuery(String.format("SELECT COL2_SMALLINT FROM teradata.%s.inttable where test = 'null'", databaseName), "VALUES (null)");
        assertQuery(String.format("SELECT CoL3_InT FROM teradata.%s.inttable where test = 'null'", databaseName), "VALUES (null)");
        // multi-column - case match
        assertQuery(String.format("SELECT TEST, col1_byteint FROM teradata.%s.inttable where test = 'null'", databaseName), "VALUES ('null', null)");
        // multi-column - case mismatch
        assertQuery(String.format("SELECT TEST, COL4_BIGINT FROM teradata.%s.inttable where test = 'null'", databaseName), "VALUES ('null', null)");
    }

    @Test
    public void selectWithDoubleQuote()
    {
        // quoted
        assertQuery(String.format("SELECT \"test\", \"col1_byteint\", \"col2_smallint\", \"col3_int\", \"col4_bigint\" FROM teradata.\"%s\".\"inttable\" where test = 'null'", databaseName), "VALUES ('null', null, null, null, null)");
        // partial quoted
        assertQuery(String.format("SELECT test, col1_byteint, col2_smallint, col3_int, col4_bigint FROM teradata.%s.\"inttable\" where test = 'null'", databaseName), "VALUES ('null', null, null, null, null)");
    }

    @Test
    public void selectVerifyPushDown()
    {
        // TODO
    }

    @Test
    public void selectVerifyNoPushDown()
    {
        // TODO
    }

    @Test
    public void selectWithNull()
    {
        // TODO
    }

    @Test
    public void selectWithOrderBy()
    {
        // TODO
    }

    @Test
    public void selectWithInnerJoin()
    {
        // TODO
        // push down
        // no-push down
    }

    @Test
    public void selectWithLeftJoin()
    {
        // TODO
        // push down
        // no-push down
    }

    @Test
    public void selectWithRightJoin()
    {
        // TODO
        // push down
        // no-push down
    }

    @Test
    public void selectWithOuterJoin()
    {
        // TODO
        // push down
        // no-push down
    }

    @Test
    public void selectFromDbcSchema()
    {
        // TODO the jdbc metadata functions filter out dbc so these won't work
    }

    @Test
    public void selectWithInNotIn()
    {
        // TODO
    }

    @Test
    public void selectWithArithmetic()
    {
        // TODO plus
        // pushdown?
    }

    @Test
    public void selectWithStringFunctions()
    {
        // TODO concat, substr, chars
    }

    @Test
    public void selectWithAgg()
    {
        // TODO sum, min, max, count, avg
    }

    @Test
    public void selectWithAggAndGroupBy()
    {
        // TODO sum, min, max, count, avg
    }

    @Test
    public void insertWithUnsupportedUnicodeCharacter()
    {
        // TODO sum, min, max, count, avg
    }
}
