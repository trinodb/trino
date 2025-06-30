/**
 * Unpublished work.
 * Copyright 2025 by Teradata Corporation. All rights reserved
 * TERADATA CORPORATION CONFIDENTIAL AND TRADE SECRET
 */

package io.trino.plugin.teradata;

import org.junit.jupiter.api.Test;

public class TestTeradataJDBCMetadata
        extends AbstractTeradataJDBCTest
{
    public TestTeradataJDBCMetadata()
    {
        super("trino_test_meta");
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
    public void showSchemas()
    {
        assertQuerySucceeds("SHOW SCHEMAS FROM teradata");
    }

    @Test
    public void showSchemaTables()
    {
        assertQuery(String.format("SHOW TABLES FROM teradata.%s", databaseName), "VALUES ('inttable'), ('chartable')");
    }

    @Test
    public void showIntTableColumns()
    {
        /*
            trino> show columns from teradata.trino_test.inttable;
                Column     |    Type     | Extra | Comment
            ---------------+-------------+-------+---------
             test          | varchar(16) |       |
             col1_byteint  | tinyint     |       |
             col2_smallint | smallint    |       |
             col3_int      | integer     |       |
             col4_bigint   | bigint      |       |
            (4 rows)
         */
        assertQuery(String.format("SHOW COLUMNS FROM teradata.%s.inttable", databaseName), "VALUES " + "('test', 'varchar(16)', '', '')," + "('col1_byteint', 'tinyint', '', '')," + "('col2_smallint', 'smallint', '', '')," + "('col3_int', 'integer', '', '')," + "('col4_bigint', 'bigint', '', '')");
        // describe appears to be same as show columns
        assertQuery(String.format("DESCRIBE teradata.%s.inttable", databaseName), "VALUES " + "('test', 'varchar(16)', '', '')," + "('col1_byteint', 'tinyint', '', '')," + "('col2_smallint', 'smallint', '', '')," + "('col3_int', 'integer', '', '')," + "('col4_bigint', 'bigint', '', '')");
    }

    @Test
    public void showCharTableColumns()
    {
        /*
        trino> show columns from teradata.trino_test.chartable;
               Column        |    Type     | Extra | Comment
        ---------------------+-------------+-------+---------
         test                | varchar(16) |       |
         col1_latinchar1     | char(1)     |       |
         col2_latinchar8     | char(8)     |       |
         col3_cschar1        | char(1)     |       |
         col4_cschar8        | char(8)     |       |
         col5_unicodechar1   | char(1)     |       |
         col6_unicodechar8   | char(8)     |       |
         col7_csunicodechar1 | char(1)     |       |
         col8_csunicodechar8 | char(8)     |       |
        (9 rows)
         */
        assertQuery(String.format("SHOW COLUMNS FROM teradata.%s.chartable", databaseName), "VALUES " + "('test', 'varchar(16)', '', '')," + "('col1_latinchar1', 'char(1)', '', '')," + "('col2_latinchar8', 'char(8)', '', '')," + "('col3_cschar1', 'char(1)', '', '')," + "('col4_cschar8', 'char(8)', '', '')," + "('col5_unicodechar1', 'char(1)', '', '')," + "('col6_unicodechar8', 'char(8)', '', '')," + "('col7_csunicodechar1', 'char(1)', '', '')," + "('col8_csunicodechar8', 'char(8)', '', '')");
        // describe appears to be same as show columns
        assertQuery(String.format("DESCRIBE teradata.%s.chartable", databaseName), "VALUES " + "('test', 'varchar(16)', '', '')," + "('col1_latinchar1', 'char(1)', '', '')," + "('col2_latinchar8', 'char(8)', '', '')," + "('col3_cschar1', 'char(1)', '', '')," + "('col4_cschar8', 'char(8)', '', '')," + "('col5_unicodechar1', 'char(1)', '', '')," + "('col6_unicodechar8', 'char(8)', '', '')," + "('col7_csunicodechar1', 'char(1)', '', '')," + "('col8_csunicodechar8', 'char(8)', '', '')");
    }

    @Test
    public void selectAllFromInvalidSchema()
    {
        assertQueryFails("SELECT * FROM teradata.invalidschema.invalidtable", ".*Schema 'invalidschema' does not exist");
    }

    @Test
    public void selectAllFromInvalidTable()
    {
        assertQueryFails(String.format("SELECT * FROM teradata.%s.invalidtable", databaseName), String.format(".*Table 'teradata.%s.invalidtable' does not exist", databaseName));
    }

    @Test
    public void selectAllFromValidTable()
    {
        assertQuerySucceeds(String.format("SELECT * FROM teradata.%s.inttable", databaseName));
        assertQuerySucceeds(String.format("SELECT * FROM teradata.%s.chartable", databaseName));
    }

    @Test
    public void showView()
    {
        // TODO
        // create table trino_test_meta.constrainttable (col1 int not null primary key, col2 int not null unique, col3 int check (col3 >= 0) constraint c1 check(col2 < col3));
        // create view trino_test_meta.joinview as (select l.test as left_test, r.test as right_test from trino_test_meta.inttable l inner join trino_test_meta.inttable r on l.test = r.test);
    }

    @Test
    public void showViewColumns()
    {
        // TODO
    }

    @Test
    public void selectAllFromView()
    {
        // TODO
    }

    @Test
    public void showTableWithRemarks()
    {
        // TODO
        // create table trino_test_meta.remarktable (col1 int, col2 int);
        // COMMENT trino_test_meta.remarktable.col2 AS 'colu2';
        /*
        trino> show columns from teradata.trino_test_meta.remarktable;
         Column |  Type   | Extra | Comment
        --------+---------+-------+---------
         col1   | integer |       |
         col2   | integer |       | colu2
        (2 rows)
         */
    }

    @Test
    public void selectAllFromTableWithDefaultValue()
    {
        // TODO
        // create table trino_test_meta.defaulttable (col1 int, col2 int default 99);
        // insert into trino_test_meta.defaulttable (col1) values (1);
    }

    @Test
    public void insertTableWithConstraint()
    {
        // TODO
        // create table trino_test_meta.constrainttable (col1 int not null primary key, col2 int not null unique, col3 int check (col3 >= 0) constraint c1 check(col2 < col3));
        // valid insert
        // invalid insert
    }
}
