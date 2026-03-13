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
package io.trino.plugin.teradata.integration;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.TestingNames;
import io.trino.testing.assertions.TrinoExceptionAssert;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.assertj.core.api.AssertProvider;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceAccessMode;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;

import static io.trino.plugin.teradata.integration.clearscape.ClearScapeEnvironmentUtils.generateUniqueEnvName;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

final class TestTeradataConnectorTest
        extends BaseJdbcConnectorTest
{
    private static final int TERADATA_OBJECT_NAME_LIMIT = 128;

    private TestingTeradataServer database;

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return database;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        database = closeAfterClass(new TestingTeradataServer(generateUniqueEnvName(getClass()), true));
        return TeradataQueryRunner.builder(database).setInitialTables(REQUIRED_TPCH_TABLES).build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_DATA,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DELETE,
                 SUPPORTS_DEREFERENCE_PUSHDOWN,
                 SUPPORTS_DROP_COLUMN,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_DROP_SCHEMA_CASCADE,
                 SUPPORTS_INSERT,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_MERGE,
                 SUPPORTS_NATIVE_QUERY,
                 SUPPORTS_NEGATIVE_DATE,
                 SUPPORTS_PREDICATE_ARITHMETIC_EXPRESSION_PUSHDOWN,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                 SUPPORTS_PREDICATE_PUSHDOWN,
                 SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_SCHEMA,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_LEVEL_DELETE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR,
                 SUPPORTS_TRUNCATE,
                 SUPPORTS_UPDATE -> false;
            case SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE -> true;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @AfterAll
    public void cleanupTestDatabase()
    {
        database = null;
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(TERADATA_OBJECT_NAME_LIMIT);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e)
                .hasMessage(format("Schema name must be shorter than or equal to '%s' characters but got '%s'", TERADATA_OBJECT_NAME_LIMIT, TERADATA_OBJECT_NAME_LIMIT + 1));
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(TERADATA_OBJECT_NAME_LIMIT);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e)
                .hasMessageMatching(format("Column name must be shorter than or equal to '%s' characters but got '%s': '.*'", TERADATA_OBJECT_NAME_LIMIT, TERADATA_OBJECT_NAME_LIMIT + 1));
    }

    @Override
    @Test
    @Tag("data_mapping")
    public void testDataMappingSmokeTest()
    {
        super.testDataMappingSmokeTest();
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(TERADATA_OBJECT_NAME_LIMIT);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e)
                .hasMessageMatching(format("Table name must be shorter than or equal to '%s' characters but got '%s'", TERADATA_OBJECT_NAME_LIMIT, TERADATA_OBJECT_NAME_LIMIT + 1));
    }

    @Override // Overriding this test case as Teradata defines varchar with a length.
    @Test
    public void testVarcharCastToDateInPredicate()
    {
        String tableName = "varchar_as_date_pred";
        try (TestTable table = newTrinoTable(
                tableName,
                "(a varchar(50))",
                ImmutableList.of(
                        "'999-09-09'",
                        "'1005-09-09'",
                        "'2005-06-06'", "'2005-06-6'", "'2005-6-06'", "'2005-6-6'", "' 2005-06-06'", "'2005-06-06 '", "' +2005-06-06'", "'02005-06-06'",
                        "'2005-09-06'", "'2005-09-6'", "'2005-9-06'", "'2005-9-6'", "' 2005-09-06'", "'2005-09-06 '", "' +2005-09-06'", "'02005-09-06'",
                        "'2005-09-09'", "'2005-09-9'", "'2005-9-09'", "'2005-9-9'", "' 2005-09-09'", "'2005-09-09 '", "' +2005-09-09'", "'02005-09-09'",
                        "'2005-09-10'", "'2005-9-10'", "' 2005-09-10'", "'2005-09-10 '", "' +2005-09-10'", "'02005-09-10'",
                        "'2005-09-20'", "'2005-9-20'", "' 2005-09-20'", "'2005-09-20 '", "' +2005-09-20'", "'02005-09-20'",
                        "'9999-09-09'",
                        "'99999-09-09'"))) {
            for (String date : ImmutableList.of("2005-09-06", "2005-09-09", "2005-09-10")) {
                for (String operator : ImmutableList.of("=", "<=", "<", ">", ">=", "!=", "IS DISTINCT FROM", "IS NOT DISTINCT FROM")) {
                    assertThat(query("SELECT a FROM %s WHERE CAST(a AS date) %s DATE '%s'".formatted(table.getName(), operator, date)))
                            .hasCorrectResultsRegardlessOfPushdown();
                }
            }
        }
        try (TestTable table = newTrinoTable(
                tableName,
                "(a varchar(50))",
                ImmutableList.of("'2005-06-bad-date'", "'2005-09-10'"))) {
            assertThat(query("SELECT a FROM %s WHERE CAST(a AS date) < DATE '2005-09-10'".formatted(table.getName())))
                    .failure().hasMessage("Value cannot be cast to date: " + "2005-06-bad-date");
            verifyResultOrFailure(
                    query("SELECT a FROM %s WHERE CAST(a AS date) = DATE '2005-09-10'".formatted(table.getName())),
                    queryAssert -> queryAssert.skippingTypesCheck().matches("VALUES '2005-09-10'"),
                    failureAssert -> failureAssert
                            .hasMessage("Value cannot be cast to date: " + "2005-06-bad-date"));
        }
        try (TestTable table = newTrinoTable(
                tableName,
                "(a varchar(50))",
                ImmutableList.of("'2005-09-10'"))) {
            // 2005-09-01, when written as 2005-09-1, is a prefix of an existing data point: 2005-09-10
            assertThat(query("SELECT a FROM %s WHERE CAST(a AS date) != DATE '2005-09-01'".formatted(table.getName())))
                    .skippingTypesCheck().matches("VALUES '2005-09-10'");
        }
    }

    @Override
    // Overridden to handle Teradata specific WITH DATA syntax for table creation
    @Test
    public void testCreateTableAsSelect()
    {
        String tableName = "test_ctas" + randomNameSuffix();
        assertUpdate(
                "CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name, regionkey FROM nation",
                "SELECT count(*) FROM nation");
        assertTableColumnNames(tableName, "name", "regionkey");
        assertThat(getTableComment(tableName)).isNull();
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate(
                "CREATE TABLE IF NOT EXISTS nation AS SELECT nationkey, regionkey FROM nation",
                0);
        assertTableColumnNames("nation", "nationkey", "name", "regionkey", "comment");

        assertCreateTableAsSelect(
                "SELECT nationkey, name, regionkey FROM nation",
                "SELECT count(*) FROM nation");

        assertCreateTableAsSelect(
                "SELECT mktsegment, sum(acctbal) x FROM customer GROUP BY mktsegment",
                "SELECT count(DISTINCT mktsegment) FROM customer");

        assertCreateTableAsSelect(
                "SELECT count(*) x FROM nation JOIN region ON nation.regionkey = region.regionkey",
                "SELECT 1");

        assertCreateTableAsSelect(
                "SELECT nationkey FROM nation ORDER BY nationkey LIMIT 10",
                "SELECT 10");

        assertCreateTableAsSelect(
                "SELECT name, nationkey, regionkey FROM nation WHERE nationkey % 2 = 0 UNION ALL " +
                        "SELECT name, nationkey, regionkey FROM nation WHERE  nationkey % 2 = 1",
                "SELECT name, nationkey, regionkey FROM nation",
                "SELECT count(*) FROM nation");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "true").build(),
                "SELECT CAST(nationkey AS BIGINT) nationkey, regionkey  FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT nationkey, regionkey FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT count(*) + 1 FROM  nation");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "SELECT CAST(nationkey AS BIGINT) nationkey, regionkey  FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT nationkey, regionkey FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT count(*) + 1 FROM nation");

        tableName = "test_ctas" + randomNameSuffix();
        assertThat(query("EXPLAIN ANALYZE CREATE TABLE " + tableName + " AS SELECT name FROM nation")).succeeds();
        assertThat(query("SELECT * from " + tableName)).matches("SELECT name FROM nation");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    // Overriding this test case as Teradata does not support negative dates.
    @Test
    public void testDateYearOfEraPredicate()
    {
        assertQuery(
                "SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'",
                "VALUES DATE '1997-09-14'");
    }

    @Override
    // Override this test case as Teradata has different syntax for creating tables with AS SELECT statement.
    @Test
    public void verifySupportsRowLevelUpdateDeclaration()
    {
        String testTableName = "test_supports_update";
        try (TestTable table = newTrinoTable(testTableName, "AS ( SELECT * FROM nation) WITH DATA")) {
            assertQueryFails(
                    "UPDATE " + table.getName() + " SET nationkey = nationkey * 100 WHERE regionkey = 2",
                    MODIFYING_ROWS_MESSAGE);
        }
    }

    @Override
    // Overriding this test case as Teradata doesn't have support to (k, v) AS VALUES in insert statement
    @Test
    public void testCharVarcharComparison()
    {
        String testTableName = "test_char_varchar";
        try (TestTable table = newTrinoTable(
                testTableName,
                "(k int, v char(3))",
                ImmutableList.of(
                        "-1, CAST(NULL AS char(3))",
                        "3, CAST('   ' AS char(3))",
                        "6, CAST('x  ' AS char(3))"))) {
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS varchar(2))",
                    "VALUES (3, '   ')");
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS varchar(4))",
                    "VALUES (3, '   ')");
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS varchar(2))",
                    "VALUES (6, 'x  ')");
        }
    }

    @Override
    // Overriding this test case as Teradata doesn't have support to (k, v) AS VALUES in insert statement
    @Test
    public void testVarcharCharComparison()
    {
        try (TestTable table = newTrinoTable(
                "test_varchar_char",
                "(k int, v char(3))",
                ImmutableList.of(
                        "-1, CAST(NULL AS varchar(3))",
                        "0, CAST('' AS varchar(3))",
                        "1, CAST(' ' AS varchar(3))",
                        "2, CAST('  ' AS varchar(3))",
                        "3, CAST('   ' AS varchar(3))",
                        "4, CAST('x' AS varchar(3))",
                        "5, CAST('x ' AS varchar(3))",
                        "6, CAST('x  ' AS " + "varchar(3))"))) {
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS char(2))",
                    "VALUES (0, '   '), (1, '   '), (2, '   '), (3, '   ')");
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS char(2))",
                    "VALUES (4, 'x  '), (5, 'x  '), (6, 'x  ')");
        }
    }

    @Override
    // Overriding to add ResourceLock to run sequential this test along with other tests labeled with TERADATA_SCHEMA to avoid issue Concurrent change conflict on database
    @Test
    @ResourceLock(value = "TERADATA_SCHEMA", mode = ResourceAccessMode.READ_WRITE)
    public void testShowCreateSchema()
    {
        super.testShowCreateSchema();
    }

    @Override
    // Overriding to add ResourceLock to run sequential this test along with other tests labeled with TERADATA_SCHEMA to avoid issue Concurrent change conflict on database
    @Test
    @ResourceLock(value = "TERADATA_SCHEMA", mode = ResourceAccessMode.READ_WRITE)
    public void testCreateSchema()
    {
        super.testCreateSchema();
    }

    @Override
    // Overriding to add ResourceLock to run sequential this test along with other tests labeled with TERADATA_SCHEMA to avoid issue Concurrent change conflict on database
    @Test
    @ResourceLock(value = "TERADATA_SCHEMA", mode = ResourceAccessMode.READ_WRITE)
    public void testCreateSchemaWithLongName()
    {
        super.testCreateSchemaWithLongName();
    }

    @Override
    // Overriding as Teradata.query method allows SELECT statements
    @Test
    public void testExecuteProcedureWithInvalidQuery()
    {
        assertQuerySucceeds("CALL system.execute('SELECT 1')");
        assertQueryFails(
                "CALL system.execute('invalid')",
                ".*Syntax error: expected something between the beginning of the request and the word 'invalid'.*");
    }

    @Override
    // Overriding to add ResourceLock to run sequential this test along with other tests labeled with TERADATA_SCHEMA to avoid issue Concurrent change conflict on database
    @Test
    @ResourceLock(value = "TERADATA_SCHEMA", mode = ResourceAccessMode.READ_WRITE)
    public void testRenameSchemaToLongName()
    {
        super.testRenameSchemaToLongName();
    }

    @Override
    // Overriding to add ResourceLock to run sequential this test along with other tests labeled with TERADATA_SCHEMA to avoid issue Concurrent change conflict on database
    @Test
    @ResourceLock(value = "TERADATA_SCHEMA", mode = ResourceAccessMode.READ_WRITE)
    public void testRenameTableAcrossSchema()
            throws Exception
    {
        super.testRenameTableAcrossSchema();
    }

    @Override
    // Overriding to add ResourceLock to run sequential this test along with other tests labeled with TERADATA_SCHEMA to avoid issue Concurrent change conflict on database
    @Test
    @ResourceLock(value = "TERADATA_SCHEMA", mode = ResourceAccessMode.READ_WRITE)
    public void testRenameTableToUnqualifiedPreservesSchema()
            throws Exception
    {
        super.testRenameTableToUnqualifiedPreservesSchema();
    }

    @Override
    // Overriding to tag this test as long_run test case to avoid running in clearscape_tests profile
    @Test
    @Tag("long_run")
    public void testSelectInformationSchemaColumns()
    {
        super.testSelectInformationSchemaColumns();
    }

    @Override
    // Overriding to tag this test as long_run test case to avoid running in clearscape_tests profile
    @Test
    @Tag("long_run")
    public void testCaseSensitiveDataMapping()
    {
        super.testCaseSensitiveDataMapping();
    }

    @Override
    // Overriding as Teradata does not support insert operations. Base implementation does not have check insert support before running the test.
    @Test
    public void testInsertIntoNotNullColumn()
    {
        abort("Skipping as connector does not support insert operations");
    }

    @Override
    // Overriding as Teradata does not support insert operations. Base implementation does not have check insert support before running the test.
    @Test
    public void testInsertWithoutTemporaryTable()
    {
        abort("Skipping as connector does not support insert operations");
    }

    @Override
    // Overriding as base test tyring to insert data but this connector not support insert operations.
    @Test
    public void testColumnName()
    {
        abort("Skipping as connector does not support column level write operations");
    }

    @Override
    // Overriding as this connector does not support creating table with UNICODE characters
    @Test
    public void testCreateTableAsSelectWithUnicode()
    {
        abort("Skipping as connector does not support creating table with UNICODE characters");
    }

    @Override
    // Overriding as Teradata does not support insert operations. Base implementation does not have check insert support before running the test.
    @Test
    public void testUpdateNotNullColumn()
    {
        abort("Skipping as connector does not support insert operations");
    }

    @Override
    // Overriding as Teradata does not support insert operations. Base implementation does not have check insert support before running the test.
    @Test
    public void testWriteBatchSizeSessionProperty()
    {
        abort("Skipping as connector does not support insert operations");
    }

    @Override
    // Overriding as Teradata does not support insert operations. Base implementation does not have check insert support before running the test.
    @Test
    public void testWriteTaskParallelismSessionProperty()
    {
        abort("Skipping as connector does not support insert operations");
    }

    @Test
    void testTeradataNumberDataType()
    {
        try (TestTable table = newTrinoTable(
                "test_number",
                "(id INTEGER,  number_col NUMBER(10,2),  number_default NUMBER,  number_large NUMBER(38,10))",
                ImmutableList.of(
                        "1, CAST(12345.67 AS NUMBER(10,2)), CAST(999999999999999 AS NUMBER), CAST(1234567890123456789012345678.1234567890 AS NUMBER(38,10))",
                        "2, CAST(-99999.99 AS NUMBER(10,2)), CAST(-123456789012345 AS NUMBER), CAST(-9999999999999999999999999999.9999999999 AS NUMBER(38,10))",
                        "3, CAST(0.00 AS NUMBER(10,2)), CAST" + "(0 AS NUMBER), CAST(0.0000000000 AS NUMBER(38,10))"))) {
            assertThat(query(format("SELECT number_col FROM %s WHERE id = 1", table.getName())))
                    .matches("VALUES CAST(12345.67 AS DECIMAL(10,2))");
            assertThat(query(format("SELECT number_default FROM %s WHERE id = 1", table.getName())))
                    .matches("VALUES CAST(999999999999999 AS DECIMAL(38,0))");
            assertThat(query(format("SELECT number_large FROM %s WHERE id = 1", table.getName())))
                    .matches("VALUES CAST(1234567890123456789012345678.1234567890 AS DECIMAL(38,10))");
            assertThat(query(format("SELECT number_col FROM %s WHERE id = 2", table.getName())))
                    .matches("VALUES CAST(-99999.99 AS DECIMAL(10,2))");
            assertThat(query(format("SELECT number_col FROM %s WHERE id = 3", table.getName())))
                    .matches("VALUES CAST(0.00 AS DECIMAL(10,2))");
        }
    }

    @Test
    void testTeradataCharacterDataType()
    {
        try (TestTable table = newTrinoTable(
                "test_character",
                "(id INTEGER,  char_col CHARACTER(5), char_default CHARACTER,  char_large CHARACTER(100))",
                ImmutableList.of(
                        "1, CAST('HELLO' AS CHARACTER(5)), CAST('A' AS CHARACTER), CAST('TERADATA' AS CHARACTER(100))",
                        "2, CAST('WORLD' AS CHARACTER(5)), CAST('B' AS CHARACTER), CAST('CHARACTER' AS CHARACTER(100))",
                        "3, CAST('' AS CHARACTER(5)), CAST('C' AS CHARACTER), CAST('' AS CHARACTER(100))"))) {
            assertThat(query(format("SELECT char_col FROM %s WHERE id = 1", table.getName())))
                    .matches("VALUES CAST('HELLO' AS CHAR(5))");
            assertThat(query(format("SELECT char_default FROM %s WHERE id = 1", table.getName())))
                    .matches("VALUES CAST('A' AS CHAR(1))");
            assertThat(query(format("SELECT char_large FROM %s WHERE id = 1", table.getName())))
                    .matches("VALUES CAST('TERADATA' AS CHAR(100))");
            assertThat(query(format("SELECT char_col FROM %s WHERE id = 3", table.getName())))
                    .matches("VALUES CAST('' AS CHAR(5))");
        }
    }

    @Override
    // Overridden to exclude data types that Teradata doesn't support or handles differently
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        return switch (typeName) {
            // skipping date as during julian->gregorian date is handled differently in Teradata.
            // tinyint, double and varchar with unbounded (need to handle special characters) are skipped and will handle it while improving write functionalities.
            case "boolean",
                 "tinyint",
                 "date",
                 "real",
                 "double",
                 "varchar",
                 "time",
                 "time(6)",
                 "timestamp",
                 "timestamp(6)",
                 "varbinary",
                 "timestamp(3) with time zone",
                 "timestamp(6) with time zone",
                 "U&'a \\000a newline'" -> Optional.empty();
            default -> Optional.of(dataMappingTestSetup);
        };
    }

    @Override
    // Overridden to use Teradata WITH DATA syntax for CREATE TABLE AS SELECT statements
    protected void assertCreateTableAsSelect(Session session, String query, String expectedQuery, String rowCountQuery)
    {
        String table = "test_ctas_" + TestingNames.randomNameSuffix();
        assertUpdate(session, "CREATE TABLE " + table + " AS ( " + query + ") WITH DATA", rowCountQuery);
        assertQuery(session, "SELECT * FROM " + table, expectedQuery);
        assertUpdate(session, "DROP TABLE " + table);
        assertThat(getQueryRunner().tableExists(session, table)).isFalse();
    }

    @Override
    // Overridden to handle Teradata schema.table naming format and table creation syntax
    protected TestTable newTrinoTable(String namePrefix, @Language("SQL") String tableDefinition, List<String> rowsToInsert)
    {
        String tableName;
        if (namePrefix.contains(".")) {
            tableName = namePrefix;
        }
        else {
            String schemaName = getSession().getSchema().orElseThrow();
            tableName = schemaName + "." + namePrefix;
        }
        return new TestTable(database, tableName, tableDefinition, rowsToInsert);
    }

    private static void verifyResultOrFailure(AssertProvider<QueryAssertions.QueryAssert> queryAssertProvider, Consumer<QueryAssertions.QueryAssert> verifyResults,
            Consumer<TrinoExceptionAssert> verifyFailure)
    {
        requireNonNull(verifyResults, "verifyResults is null");
        requireNonNull(verifyFailure, "verifyFailure is null");
        QueryAssertions.QueryAssert queryAssert = assertThat(queryAssertProvider);
        verifyResults.accept(queryAssert);
    }
}
