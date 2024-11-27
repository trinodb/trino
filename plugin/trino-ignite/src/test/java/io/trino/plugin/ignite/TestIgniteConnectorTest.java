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
package io.trino.plugin.ignite;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

@Isolated
public class TestIgniteConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingIgniteServer igniteServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.igniteServer = closeAfterClass(TestingIgniteServer.getInstance()).get();
        return IgniteQueryRunner.builder(igniteServer)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return igniteServer::execute;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN,
                 SUPPORTS_JOIN_PUSHDOWN,
                 SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN_WITH_LIKE,
                 SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR -> true;
            case SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT,
                 SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                 SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                 SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                 SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                 SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                 SUPPORTS_ARRAY,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                 SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                 SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
                 SUPPORTS_MAP_TYPE,
                 SUPPORTS_NATIVE_QUERY,
                 SUPPORTS_NEGATIVE_DATE,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_ROW_TYPE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TRUNCATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    public void testLikeWithEscape()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_like_with_escape",
                "(id int, a varchar(4))",
                List.of(
                        "1, 'abce'",
                        "2, 'abcd'",
                        "3, 'a%de'"))) {
            String tableName = testTable.getName();

            assertThat(query("SELECT * FROM " + tableName + " WHERE a LIKE 'a%'"))
                    .isFullyPushedDown();
            assertThat(query("SELECT * FROM " + tableName + " WHERE a LIKE '%c%' ESCAPE '\\'"))
                    .matches("VALUES (1, 'abce'), (2, 'abcd')")
                    .isNotFullyPushedDown(node(FilterNode.class, node(TableScanNode.class)));

            assertThat(query("SELECT * FROM " + tableName + " WHERE a LIKE 'a\\%d%' ESCAPE '\\'"))
                    .matches("VALUES (3, 'a%de')")
                    .isNotFullyPushedDown(node(FilterNode.class, node(TableScanNode.class)));

            assertThatThrownBy(() -> onRemoteDatabase().execute("SELECT * FROM " + tableName + " WHERE a LIKE 'a%' ESCAPE '\\'"))
                    .hasMessageContaining("Failed to execute statement");
        }
    }

    @Test
    public void testIsNullPredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE name IS NULL")).isFullyPushedDown();
        assertThat(query("SELECT nationkey FROM nation WHERE name IS NULL OR name = 'a' OR regionkey = 4")).isFullyPushedDown();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_is_null_predicate_pushdown",
                "(a_int integer, a_varchar varchar(1))",
                List.of(
                        "1, 'A'",
                        "2, 'B'",
                        "1, NULL",
                        "2, NULL"))) {
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE a_varchar IS NULL OR a_int = 1")).isFullyPushedDown();
        }
    }

    @Test
    public void testIsNotNullPredicatePushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE name IS NOT NULL OR regionkey = 4")).isFullyPushedDown();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_is_not_null_predicate_pushdown",
                "(a_int integer, a_varchar varchar(1))",
                List.of(
                        "1, 'A'",
                        "2, 'B'",
                        "1, NULL",
                        "2, NULL"))) {
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE a_varchar IS NOT NULL OR a_int = 1")).isFullyPushedDown();
        }
    }

    @Test
    public void testNotExpressionPushdown()
    {
        assertThat(query("SELECT nationkey FROM nation WHERE NOT(name LIKE '%A%')")).isFullyPushedDown();

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_is_not_predicate_pushdown",
                "(a_int integer, a_varchar varchar(2))",
                List.of(
                        "1, 'Aa'",
                        "2, 'Bb'",
                        "1, NULL",
                        "2, NULL"))) {
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE NOT(a_varchar LIKE 'A%') OR a_int = 2")).isFullyPushedDown();
            assertThat(query("SELECT a_int FROM " + table.getName() + " WHERE NOT(a_varchar LIKE 'A%' OR a_int = 2)")).isFullyPushedDown();
        }
    }

    @Test
    public void testDatabaseMetadataSearchEscapedWildCardCharacters()
    {
        // wildcard characters on schema name
        assertQuerySucceeds("SHOW TABLES FROM public");
        assertQueryFails("SHOW TABLES FROM \"publi_\"", ".*Schema 'publi_' does not exist");
        assertQueryFails("SHOW TABLES FROM \"pu%lic\"", ".*Schema 'pu%lic' does not exist");

        String tableNameSuffix = randomNameSuffix();
        String normalTableName = "testxsearch" + tableNameSuffix;
        String underscoreTableName = "\"" + "test_search" + tableNameSuffix + "\"";
        String percentTableName = "\"" + "test%search" + tableNameSuffix + "\"";
        try {
            assertUpdate("CREATE TABLE " + normalTableName + "(a int, b int, c int) WITH (primary_key = ARRAY['a'])");
            assertUpdate("CREATE TABLE " + underscoreTableName + "(a int, b int, c int) WITH (primary_key = ARRAY['b'])");
            assertUpdate("CREATE TABLE " + percentTableName + " (a int, b int, c int) WITH (primary_key = ARRAY['c'])");

            // wildcard characters on table name
            assertThat((String) computeScalar("SHOW CREATE TABLE " + normalTableName)).contains("primary_key = ARRAY['a']");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + underscoreTableName)).contains("primary_key = ARRAY['b']");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + percentTableName)).contains("primary_key = ARRAY['c']");
            assertQueryFails("SHOW CREATE TABLE " + "\"test%\"", ".*Table 'ignite.public.\"test%\"' does not exist");
            assertQueryFails("SHOW COLUMNS FROM " + "\"test%\"", ".*Table 'ignite.public.\"test%\"' does not exist");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + normalTableName);
            assertUpdate("DROP TABLE IF EXISTS " + underscoreTableName);
            assertUpdate("DROP TABLE IF EXISTS " + percentTableName);
        }
    }

    @Test
    public void testCreateTableSqlInjection()
    {
        assertUpdate("CREATE TABLE a1 (id int, a varchar)");
        assertUpdate("CREATE TABLE x2 (id int, a varchar)");
        assertUpdate("CREATE TABLE x3 (id int, a varchar)");
        assertQuery("SHOW TABLES IN ignite.public LIKE 'a%'", "VALUES ('a1')");

        // injection on table name
        assertUpdate("CREATE TABLE \"test (c1 int not null, c2 int, primary key(c1)); DROP TABLE public.a1;\" (c1 date)");
        assertQuery("SHOW TABLES IN ignite.public LIKE 'a%'", "VALUES ('a1')");

        // injection on column name
        assertUpdate("CREATE TABLE test (\"test (c1 int not null, c2 int, primary key(c1)); DROP TABLE public.a1;\" date)");
        assertQuery("SHOW TABLES IN ignite.public LIKE 'a%'", "VALUES ('a1')");
    }

    @Test
    public void testCreateTableWithCommaPropertyColumn()
    {
        // Test that Ignite not support column name contains quote
        String tableWithQuote = "create_table_with_unsupported_quote_column";
        String tableDefinitionWithQuote = "(`a\"b` bigint primary key, c varchar)";
        assertThatThrownBy(() -> onRemoteDatabase().execute("CREATE TABLE " + tableWithQuote + tableDefinitionWithQuote))
                .rootCause()
                .hasMessageContaining("Failed to parse query");

        // Test the property column with comma
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                "create_table_with_comma_column",
                "(`a,b` bigint primary key, `c,d` bigint, `x` varchar(79))",
                List.of("1, 1, 'a'", "2, 2, 'b'", "3, 3, null"))) {
            String pattern = "CREATE TABLE %s.%s.%s (\n" +
                    "   \"a,b\" bigint,\n" +
                    "   \"c,d\" bigint,\n" +
                    "   x varchar(79)\n" +
                    ")\n" +
                    "WITH (\n" +
                    "   primary_key = ARRAY['a,b']\n" +
                    ")";
            String tableName = testTable.getName();
            assertQuery("SELECT \"a,b\" FROM " + tableName + " where \"a,b\" < 2", "values (1)");
            assertQuery("SELECT \"a,b\" FROM " + tableName + " where \"a,b\" > 1", "values (2), (3)");

            assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue())
                    .isEqualTo(format(pattern, catalog, schema, tableName));
        }
    }

    @Test
    public void testCreateTableWithNonExistingPrimaryKey()
    {
        String tableName = "test_invalid_primary_key" + randomNameSuffix();
        assertQueryFails("CREATE TABLE " + tableName + "(a bigint) WITH (primary_key = ARRAY['not_existing_column'])",
                "Column 'not_existing_column' specified in property 'primary_key' doesn't exist in table");

        assertQueryFails("CREATE TABLE " + tableName + "(a bigint) WITH (primary_key = ARRAY['dummy_id'])",
                "Column 'dummy_id' specified in property 'primary_key' doesn't exist in table");

        assertQueryFails("CREATE TABLE " + tableName + "(a bigint) WITH (primary_key = ARRAY['A'])",
                "Column 'A' specified in property 'primary_key' doesn't exist in table");
    }

    @Test
    public void testCreateTableWithAllProperties()
    {
        String tableWithAllProperties = "test_create_with_all_properties";
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableWithAllProperties + " (a bigint, b double, c varchar, d date) WITH (primary_key = ARRAY['a', 'b'])");
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "public.tbl",
                "(col_required bigint," +
                        "col_nullable bigint," +
                        "col_default bigint DEFAULT 43," +
                        "col_nonnull_default bigint DEFAULT 42," +
                        "col_required2 bigint NOT NULL, " +
                        "dummy_id varchar NOT NULL primary key)");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("CREATE TABLE ignite.public.orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   primary_key = ARRAY['dummy_id']\n" +
                        ")");
    }

    @Test
    public void testAvgDecimalExceedingSupportedPrecision()
    {
        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_avg_decimal_exceeding_supported_precision",
                "(a decimal(38, 38), b bigint)",
                List.of(
                        "CAST ('0.12345671234567123456712345671234567121' AS decimal(38, 38)), 1",
                        "CAST ('0.12345671234567123456712345671234567122' AS decimal(38, 38)), 2",
                        "CAST ('0.12345671234567123456712345671234567123' AS decimal(38, 38)), 3",
                        "CAST ('0.12345671234567123456712345671234567124' AS decimal(38, 38)), 4",
                        "CAST ('0.12345671234567123456712345671234567125' AS decimal(38, 38)), 5",
                        "CAST ('0.12345671234567123456712345671234567126' AS decimal(38, 38)), 6",
                        "CAST ('0.12345671234567123456712345671234567127' AS decimal(38, 38)), 7"))) {
            assertThat(query("SELECT avg(a) avg_a  FROM " + testTable.getName()))
                    .matches("SELECT CAST ('0.12345671234567123456712345671234567124' AS decimal(38, 38))");
            assertThat(query(format("SELECT avg(a) avg_a FROM %s WHERE b <= 2", testTable.getName())))
                    .matches("SELECT CAST ('0.123456712345671234567123456712345671215' AS decimal(38, 38))");
        }
    }

    @Override
    protected TestTable createAggregationTestTable(String name, List<String> rows)
    {
        return new TestTable(
                onRemoteDatabase(),
                name,
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10), t_double double, a_bigint bigint primary key)",
                rows);
    }

    @Override
    protected TestTable createTableWithDoubleAndRealColumns(String name, List<String> rows)
    {
        return new TestTable(
                onRemoteDatabase(),
                name,
                "(t_double double, u_double double, v_real real, w_real real primary key)",
                rows);
    }

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        // https://issues.apache.org/jira/browse/IGNITE-18102
        if ("a.dot".equals(columnName)) {
            return Optional.empty();
        }

        return Optional.of(columnName);
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        String errorMessage = nullToEmpty(exception.getMessage());
        if (columnName.equals("a\"quote")) {
            return errorMessage.contains("Failed to parse query.");
        }

        return errorMessage.contains("Failed to complete exchange process");
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e).hasMessage("Schema change operation failed: Thread got interrupted while trying to acquire table lock.");
    }

    @Test
    @Override
    public void testDropAndAddColumnWithSameName()
    {
        // Override because Ignite can access old data after dropping and adding a column with same name
        executeExclusively(() -> {
            try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_add_column", "AS SELECT 1 x, 2 y, 3 z")) {
                assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN y");
                assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 3)");

                assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN y int");
                assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 3, 2)");
            }
        });
    }

    @Test
    @Override
    public void testAddColumn()
    {
        // Isolate this test to avoid problem described in https://github.com/trinodb/trino/issues/16671
        executeExclusively(super::testAddColumn);
    }

    @Test
    @Override
    public void testDropColumn()
    {
        // Isolate this test to avoid problem described in https://github.com/trinodb/trino/issues/16671
        executeExclusively(super::testDropColumn);
    }

    @Test
    @Override
    public void testAlterTableAddLongColumnName()
    {
        // Isolate this test to avoid problem described in https://github.com/trinodb/trino/issues/16671
        executeExclusively(super::testAlterTableAddLongColumnName);
    }

    @Test
    @Override
    public void testAddAndDropColumnName()
    {
        // Isolate this test to avoid problem described in https://github.com/trinodb/trino/issues/16671
        executeExclusively(super::testAddAndDropColumnName);
    }

    @Override
    protected TestTable simpleTable()
    {
        return new TestTable(onRemoteDatabase(), format("%s.simple_table", getSession().getSchema().orElseThrow()), "(col BIGINT, id bigint primary key)", ImmutableList.of("1, 1", "2, 2"));
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        // Ignite will map char to varchar, skip
        abort("Ignite map char to varchar, skip test");
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("Failed to insert data: Null value is not allowed for column '%s'", columnName.toUpperCase(Locale.ENGLISH));
    }

    @Test
    @Override
    public void testCharTrailingSpace()
    {
        abort("Ignite not support char trailing space");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        switch (dataMappingTestSetup.getTrinoTypeName()) {
            case "date":
                // Ignite doesn't support these days
                if (dataMappingTestSetup.getSampleValueLiteral().equals("DATE '0001-01-01'") || dataMappingTestSetup.getSampleValueLiteral().equals("DATE '1582-10-05'")) {
                    return Optional.empty();
                }
                break;

            case "time":
            case "time(6)":
            case "timestamp":
            case "timestamp(6)":
            case "timestamp(3) with time zone":
            case "timestamp(6) with time zone":
                return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        // Override because the connector throws an exception instead of an empty result when the value is out of supported range
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
        assertQueryFails(
                "SELECT * FROM orders WHERE orderdate = DATE '-1996-09-14'",
                errorMessageForDateOutOfRange("-1996-09-14"));
    }

    @Override
    protected String errorMessageForInsertNegativeDate(String date)
    {
        return errorMessageForDateOutOfRange(date);
    }

    @Override
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        return errorMessageForDateOutOfRange(date);
    }

    private String errorMessageForDateOutOfRange(String date)
    {
        return "Date must be between 1970-01-01 and 9999-12-31 in Ignite: " + date;
    }

    @Test
    @Override
    public void testSelectInformationSchemaColumns()
    {
        // Isolate this test to avoid problem described in https://github.com/trinodb/trino/issues/16671
        executeExclusively(super::testSelectInformationSchemaColumns);
    }

    @Test
    @Override // Override because Ignite requires primary keys
    public void testExecuteProcedure()
    {
        String tableName = "test_execute" + randomNameSuffix();
        String schemaTableName = getSession().getSchema().orElseThrow() + "." + tableName;

        assertUpdate("CREATE TABLE " + schemaTableName + "(id int, data int) WITH (primary_key = ARRAY['id'])");
        try {
            assertUpdate("CALL system.execute('INSERT INTO " + schemaTableName + " VALUES (1, 10)')");
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES (1, 10)");

            assertUpdate("CALL system.execute('UPDATE " + schemaTableName + " SET data = 100')");
            assertQuery("SELECT * FROM " + schemaTableName, "VALUES (1, 100)");

            assertUpdate("CALL system.execute('DELETE FROM " + schemaTableName + "')");
            assertQueryReturnsEmptyResult("SELECT * FROM " + schemaTableName);

            assertUpdate("CALL system.execute('DROP TABLE " + schemaTableName + "')");
            assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaTableName);
        }
    }
}
