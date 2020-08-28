/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.google.sheets;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.prestosql.plugin.google.sheets.TestSheetsPlugin.TEST_METADATA_SHEET_ID;
import static io.prestosql.plugin.google.sheets.TestSheetsPlugin.getTestCredentialsPath;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

public class TestGoogleSheets
        extends AbstractTestQueryFramework
{
    protected static final String GOOGLE_SHEETS = "gsheets";
    protected static final String SHEET_RANGE = "Sheet1";
    protected static final String SHEET_VALUE_INPUT_OPTION = "RAW";
    protected static final String DRIVE_PERMISSION_TYPE = "user";
    protected static final String DRIVE_PERMISSION_ROLE = "writer";
    protected static final String DRIVE_PERMISSION_EMAIL_ADDRESS = "cla@prestosql.io";

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog(GOOGLE_SHEETS)
                .setSchema("default")
                .build();
    }

    @Override
    protected QueryRunner createQueryRunner()
    {
        QueryRunner queryRunner;
        try {
            SheetsPlugin sheetsPlugin = new SheetsPlugin();
            queryRunner = DistributedQueryRunner.builder(createSession()).build();
            queryRunner.installPlugin(sheetsPlugin);
            queryRunner.createCatalog(GOOGLE_SHEETS, GOOGLE_SHEETS, new ImmutableMap.Builder<String, String>()
                    .put("credentials-path", getTestCredentialsPath())
                    .put("metadata-sheet-id", "foo_bar_sheet_id#Sheet1")
                    .put("sheets-data-max-cache-size", "2000")
                    .put("sheets-data-expire-after-write", "10m")
                    .put("sheets-range","Sheet1")
                    .put("sheets-value-input-option","RAW")
                    .put("drive-permission-type","user")
                    .put("drive-permission-role","writer")
                    .put("drive-permission-email-address","<<insertYourEmailHere@InsertYourEmailHere.com>>")
                    .build());
        }
        catch (Exception e) {
            throw new IllegalStateException(e.getMessage());
        }
        return queryRunner;
    }

    @Test
    public void testDescTable()
    {
        assertQuery("desc number_text", "SELECT * FROM (VALUES('number','integer','',''), ('text','varchar','',''))");
        assertQuery("desc metadata_table", "SELECT * FROM (VALUES('table name','varchar','',''), ('sheetid_sheetname','varchar','',''), "
                + "('owner','varchar','',''), ('notes','varchar','',''),('column_types','varchar','',''))");
    }

    @Test
    public void testSelectFromTable()
    {
        assertQuery("SELECT count(*) FROM number_text", "SELECT 5");
        assertQuery("SELECT number FROM number_text", "SELECT * FROM (VALUES '1','2','3','4','5')");
        assertQuery("SELECT text FROM number_text", "SELECT * FROM (VALUES 'one','two','three','four','five')");
        assertQuery("SELECT * FROM number_text", "SELECT * FROM (VALUES ('1','one'), ('2','two'), ('3','three'), ('4','four'), ('5','five'))");
    }

    @Test
    public void testSelectFromTableIgnoreCase()
    {
        assertQuery("SELECT count(*) FROM NUMBER_TEXT", "SELECT 5");
        assertQuery("SELECT number FROM Number_Text", "SELECT * FROM (VALUES '1','2','3','4','5')");
    }

    @Test
    public void testQueryingUnknownSchemaAndTable()
    {
        assertQueryFails("select * from gsheets.foo.bar", "line 1:15: Schema foo does not exist");
        assertQueryFails("select * from gsheets.default.foo_bar_table", "line 1:15: Table gsheets.default.foo_bar_table does not exist");
    }

    @Test
    public void testTableWithRepeatedAndMissingColumnNames()
    {
        assertQuery("desc table_with_duplicate_and_missing_column_names", "SELECT * FROM (VALUES('a','varchar','','')," +
                " ('column_1','varchar','',''), ('b','varchar','',''), " +
                "('c','varchar','',''), ('column_2','varchar','',''), ('d','varchar','',''))");
    }

    @Test
    public void testCreateTableAsQuery()
    {
        String createSql = "" +
                "CREATE TABLE test_create_table_as AS " +
                "SELECT" +
                " 'foo' _varchar" +
                ", cast(1 as integer) _integer";
        assertQuerySucceeds(createSql);
        MaterializedResult result = getQueryRunner()
                .execute(getSession(), "select * from metadata_table where \"table name\" = " +
                        "'test_create_table_as' and notes = 'test_create_table_as#Active'")
                .toTestTypes();
        assertEquals(result.getRowCount(), 1);
        MaterializedRow row = result.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "test_create_table_as");
        assertNotNull(row.getField(1));
        assertNotNull(row.getField(2), "prestosql");
        assertEquals(row.getField(3), "test_create_table_as#Active");
        assertEquals(row.getField(4), "test_create_table_as#varchar,integer");
        MaterializedResult insertTableResults = getQueryRunner()
                .execute(getSession(), "select * from test_create_table_as")
                .toTestTypes();
        assertEquals(insertTableResults.getRowCount(), 1);
        MaterializedRow insertRow = insertTableResults.getMaterializedRows().get(0);
        assertEquals(insertRow.getField(0), "foo");
        assertEquals(insertRow.getField(1), 1);
        assertUpdate("DROP TABLE test_create_table_as");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_table_as"));
    }

    @Test
    public void testCreateTableQuery()
    {
        String createSql = "" +
                "CREATE TABLE test_create_table " +
                "(" +
                "  name varchar" +
                ", age int" +
                ")";
        assertQuerySucceeds(createSql);
        MaterializedResult results = getQueryRunner()
                .execute(getSession(), "select * from metadata_table where \"table name\" = " +
                        "'test_create_table' and notes = 'test_create_table#Active'")
                .toTestTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "test_create_table");
        assertNotNull(row.getField(1));
        assertNotNull(row.getField(2), "prestosql");
        assertEquals(row.getField(3), "test_create_table#Active");
        assertEquals(row.getField(4), "test_create_table#varchar,integer");
        assertUpdate("DROP TABLE test_create_table");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_table"));
    }

    @Test
    public void testDropTableQuery()
    {
        String createSql = "" +
                "CREATE TABLE test_drop_table " +
                "(" +
                "  name varchar" +
                ", age int" +
                ")";
        getQueryRunner().execute(getSession(), createSql);
        assertUpdate("DROP TABLE test_drop_table");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_table"));
    }

    @Test
    public void testInsertQuery()
    {
        String createSql = "" +
                "CREATE TABLE test_insert_table " +
                "(" +
                "  name varchar" +
                ", age int" +
                ")";
        assertQuerySucceeds(createSql);
        String insertSql = "" +
                "INSERT INTO test_insert_table VALUES('foo',1)";
        assertUpdate(insertSql, 1);
        MaterializedResult results = getQueryRunner().execute(getSession(), "select * from test_insert_table")
                .toTestTypes();
        assertUpdate("DROP TABLE test_insert_table");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_insert_table"));
    }

    @Test
    public void createTableWithEveryType()
    {
        String query = "" +
                "CREATE TABLE test_types_table AS " +
                "SELECT" +
                " 'foo' _varchar" +
                ", cast(1 as integer) _integer" +
                ", cast(1 as bigint) _bigint" +
                ", 3.14E0 _double" +
                ", 1 tinyint";
        assertUpdate(query, 1);
        MaterializedResult results = getQueryRunner().execute(getSession(), "SELECT * FROM test_types_table").toTestTypes();
        assertEquals(results.getRowCount(), 1);
        MaterializedRow row = results.getMaterializedRows().get(0);
        assertEquals(row.getField(0), "foo");
        assertEquals(row.getField(1), 1);
        assertEquals(row.getField(2), 1L);
        assertEquals(row.getField(3), 3.14);
        assertEquals(row.getField(4), 1);
        assertUpdate("DROP TABLE test_types_table");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_types_table"));
    }

    @Test
    public void testInsertWithEveryType()
    {
        String createSql = "" +
                "CREATE TABLE test_insert_types_table " +
                "(" +
                "  vc varchar" +
                ", it integer" +
                ", bi bigint" +
                ", d double" +
                ", b tinyint" +
                ")";
        getQueryRunner().execute(getSession(), createSql);

        String insertSql = "" +
                "INSERT INTO test_insert_types_table " +
                "SELECT" +
                " 'foo' _varchar" +
                ", cast(1 as integer) _integer" +
                ", cast(1 as bigint) _bigint" +
                ", 3.14E0 _double" +
                ", 1 _tinyint";
        assertUpdate(insertSql, 1);
        assertUpdate("DROP TABLE test_insert_types_table");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_insert_types_table"));
    }

    @Test
    public void testAlterTableAddColumnQuery()
    {
        String createSql = "" +
                "CREATE TABLE test_alter_table " +
                "(" +
                "  vc varchar" +
                ", it integer" +
                ")";
        getQueryRunner().execute(getSession(), createSql);

        String alterSql = "ALTER TABLE test_alter_table ADD COLUMN d double";
        assertQuerySucceeds(alterSql);
        assertQuery("desc test_alter_table", "SELECT * FROM (VALUES('vc','varchar','','')," +
                " ('it','integer','',''), ('d','double','',''))");
        assertUpdate("DROP TABLE test_alter_table");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_insert_types_table"));
    }

    @Test
    public void testRelationalQueries()
    {
        assertQuery("SELECT * FROM test_type_queries where _varchar = 'test'",
                "SELECT * FROM (VALUES ('test','1', '1.2','1980-05-07','2000-05-07 06:22:33.000','07:22:33.000'))");
        assertQuery("SELECT * FROM test_type_queries where _int > 0",
                "SELECT * FROM (VALUES ('test','1', '1.2','1980-05-07','2000-05-07 06:22:33.000','07:22:33.000'))");
        assertQueryReturnsEmptyResult("SELECT * FROM test_type_queries where _int > 1");
        assertQuery("SELECT * FROM test_type_queries where _float >= 1.2",
                "SELECT * FROM (VALUES ('test','1', '1.2','1980-05-07','2000-05-07 06:22:33.000','07:22:33.000'))");
        assertQueryReturnsEmptyResult("SELECT * FROM test_type_queries where _float > 1.2");
        assertQuery("SELECT * FROM test_type_queries where _date between DATE '1980-05-07' and NOW()",
                "SELECT * FROM (VALUES ('test','1', '1.2','1980-05-07','2000-05-07 06:22:33.000','07:22:33.000'))");
        assertQueryReturnsEmptyResult("SELECT * FROM test_type_queries where _date between DATE '1981-05-07' and NOW()");
        assertQuery("SELECT * FROM test_type_queries where _timestamp >= TIMESTAMP '2000-05-07 06:22:33.000'",
                "SELECT * FROM (VALUES ('test','1', '1.2','1980-05-07','2000-05-07 06:22:33.000','07:22:33.000'))");
        assertQueryReturnsEmptyResult("SELECT * FROM test_type_queries where _timestamp >= TIMESTAMP '2000-05-07 06:23:33.000'");
        assertQuery("SELECT * FROM test_type_queries where _time >= TIME '07:22:33'",
                "SELECT * FROM (VALUES ('test','1', '1.2','1980-05-07','2000-05-07 06:22:33.000','07:22:33.000'))");
        assertQueryReturnsEmptyResult("SELECT * FROM test_type_queries where _time > TIME '07:22:35'");
    }

    @Test
    public void testListTable()
    {
        assertQuery("show tables", "SELECT * FROM (VALUES 'metadata_table', 'number_text', " +
                "'table_with_duplicate_and_missing_column_names', 'test_type_queries')");
        assertQueryReturnsEmptyResult("SHOW TABLES IN gsheets.information_schema LIKE 'number_text'");
    }
}
