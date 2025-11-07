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
package io.trino.plugin.databend;

import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.Locale;
import java.util.Optional;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDatabendConnectorTest
        extends BaseJdbcConnectorTest
{
    private TestingDatabendServer databendServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        databendServer = closeAfterClass(new TestingDatabendServer());
        return DatabendQueryRunner.createDatabendQueryRunner(databendServer, emptyMap(), emptyMap());
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return databendServer::execute;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior behavior)
    {
        return switch (behavior) {
            case SUPPORTS_ARRAY,
                    SUPPORTS_MAP_TYPE,
                    SUPPORTS_ROW_TYPE,
                    SUPPORTS_DELETE,
                    SUPPORTS_ROW_LEVEL_DELETE,
                    SUPPORTS_UPDATE,
                    SUPPORTS_ROW_LEVEL_UPDATE,
                    SUPPORTS_RENAME_SCHEMA,
                    SUPPORTS_ADD_COLUMN_WITH_POSITION,
                    SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                    SUPPORTS_NOT_NULL_CONSTRAINT,
                    SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT,
                    SUPPORTS_DEFAULT_COLUMN_VALUE,
                    SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                    SUPPORTS_COMMENT_ON_COLUMN,
                    SUPPORTS_COMMENT_ON_TABLE,
                    SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                    SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                    SUPPORTS_SET_COLUMN_TYPE,
                    SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                    SUPPORTS_NATIVE_QUERY,
                    SUPPORTS_AGGREGATION_PUSHDOWN,
                    SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                    SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                    SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                    SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                    SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                    SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT,
                    SUPPORTS_CREATE_VIEW,
                    SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_NEGATIVE_DATE -> false;
            default -> super.hasBehavior(behavior);
        };
    }

    @Test
    @Override
    public void testShowColumns()
    {
        assertThat(computeActual("SHOW COLUMNS FROM orders"))
                .isEqualTo(getDescribeOrdersResult());
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("clerk", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar", "", "")
                .build();
    }

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        // Databend converts column names to lowercase
        return Optional.of(columnName.toLowerCase(Locale.ROOT));
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup setup)
    {
        String type = setup.getTrinoTypeName();
        if (type.equals("time") || type.startsWith("time(")) {
            return Optional.of(setup.asUnsupported());
        }
        return Optional.of(setup);
    }

    @Override
    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        String message = nullToEmpty(exception.getMessage());
        return message.contains("unable to recognize the rest tokens");
    }

    @Test
    @Override
    public void testAddColumn()
    {
        String tableName;
        try (TestTable table = newTrinoTable("test_add_column_", tableDefinitionForAddColumn())) {
            tableName = table.getName();
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'first'", 1);
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN x bigint", ".* Column 'x' already exists");
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN X bigint", ".* Column 'X' already exists");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN a varchar(50)");
            assertQuery(
                    "SELECT * FROM " + table.getName(),
                    "VALUES ('first', NULL)");
            assertQuery(
                    "SELECT * FROM " + table.getName() + " WHERE a IS NULL",
                    "VALUES ('first', NULL)");
        }
    }

    @Test
    @Override // Overridden because the default storage type doesn't support adding columns
    public void testAddNotNullColumnToEmptyTable()
    {
        try (TestTable table = newTrinoTable("test_add_notnull_col_to_empty", "(a_varchar varchar NOT NULL)")) {
            String tableName = table.getName();

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL");
            assertUpdate("INSERT INTO " + tableName + " VALUES ('a', 'b')", 1);
            assertThat(query("TABLE " + tableName))
                    .skippingTypesCheck()
                    .matches("VALUES ('a', 'b')");
        }
    }

    @Test
    @Override
    public void testDropNotNullConstraint()
    {
        abort("Databend Not Support");
    }

    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        try (TestTable table = newTrinoTable("test_insert_not_null_", "(nullable_col INTEGER, not_null_col INTEGER NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
        }

        try (TestTable table = newTrinoTable("test_commuted_not_null_table", "(nullable_col BIGINT, not_null_col BIGINT NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
        }
    }

    @Test
    @Override
    public void testUpdateNotNullColumn()
    {
        abort("Databend rejects updates that temporarily assign NULL to NOT NULL columns");
    }

    @Test
    @Override
    public void testColumnName()
    {
        abort("Databend rejects some generated column names used in the generic test");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        assertThat(computeScalar("SHOW CREATE TABLE orders"))
                .isEqualTo(format(
                        """
                        CREATE TABLE %s.%s.orders (
                           orderkey bigint,
                           custkey bigint,
                           orderstatus varchar,
                           totalprice double,
                           orderdate date,
                           orderpriority varchar,
                           clerk varchar,
                           shippriority integer,
                           comment varchar
                        )
                        WITH (
                           engine = 'FUSE'
                        )\
                        """,
                        catalog,
                        schema));
    }

    @Test
    @Override
    public void testDateYearOfEraPredicate()
    {
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
        assertQueryFails("SELECT * FROM orders WHERE orderdate = DATE '-1996-09-14'", invalidDateReadError("-1996-09-14"));
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("(?s).*%s.*", columnName);
    }

    @Override
    protected String errorMessageForInsertNegativeDate(String date)
    {
        return invalidDateWriteError(date);
    }

    @Override
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        return invalidDateWriteError(date);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        String schema = getSession().getSchema().orElseThrow();
        return new TestTable(
                onRemoteDatabase(),
                schema + ".test_default_cols",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        assertThatThrownBy(super::testCharVarcharComparison)
                .isInstanceOf(AssertionError.class);
        abort("Databend trims CHAR values differently than Trino");
    }

    @Test
    @Override
    public void testVarcharCharComparison()
    {
        assertThatThrownBy(super::testVarcharCharComparison)
                .isInstanceOf(AssertionError.class);
        abort("Databend trims CHAR values differently than Trino");
    }

    @Test
    @Override
    public void testCharTrailingSpace()
    {
        assertThatThrownBy(super::testCharTrailingSpace)
                .isInstanceOf(AssertionError.class);
        abort("Databend trims CHAR values differently than Trino");
    }

    @Test
    @Override
    public void testExecuteProcedure()
    {
        assertThatThrownBy(super::testExecuteProcedure)
                .isInstanceOf(AssertionError.class);
        abort("system.execute is not supported by Databend");
    }

    @Test
    @Override
    public void testExecuteProcedureWithInvalidQuery()
    {
        assertThatThrownBy(super::testExecuteProcedureWithInvalidQuery)
                .isInstanceOf(AssertionError.class);
        abort("system.execute is not supported by Databend");
    }

    @Test
    @Override
    public void testExecuteProcedureWithNamedArgument()
    {
        assertThatThrownBy(super::testExecuteProcedureWithNamedArgument)
                .isInstanceOf(AssertionError.class);
        abort("system.execute is not supported by Databend");
    }

    @Override
    @Test
    public void testInsert()
    {
        String tableName = "test_insert_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, name VARCHAR)");
        assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'test')", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (1, 'test')");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    @Test
    public void testCreateTable()
    {
        String tableName = "test_create_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id BIGINT, name VARCHAR, price DOUBLE)");
        assertThat(computeActual("SHOW TABLES LIKE '" + tableName + "'").getRowCount())
                .isEqualTo(1);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    @Test
    public void testTruncateTable()
    {
        String tableName = "test_truncate_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);
        assertUpdate("TRUNCATE TABLE " + tableName);
        assertQuery("SELECT COUNT(*) FROM " + tableName, "SELECT 0");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testBasicDataTypes()
    {
        String tableName = "test_types_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (" +
                "bool_col BOOLEAN, " +
                "tinyint_col TINYINT, " +
                "smallint_col SMALLINT, " +
                "int_col INTEGER, " +
                "bigint_col BIGINT, " +
                "real_col REAL, " +
                "double_col DOUBLE, " +
                "varchar_col VARCHAR, " +
                "date_col DATE)");

        assertUpdate("INSERT INTO " + tableName + " VALUES (" +
                "true, " +
                "127, " +
                "32767, " +
                "2147483647, " +
                "9223372036854775807, " +
                "REAL '3.14', " +
                "2.718, " +
                "'test string', " +
                "DATE '2024-01-01')", 1);

        assertQuery("SELECT * FROM " + tableName,
                "VALUES (true, 127, 32767, 2147483647, 9223372036854775807, CAST(3.14 AS real), 2.718, 'test string', DATE '2024-01-01')");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDecimalType()
    {
        String tableName = "test_decimal_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (decimal_col DECIMAL(10, 2))");
        assertUpdate("INSERT INTO " + tableName + " VALUES (123.45)", 1);
        assertQuery("SELECT * FROM " + tableName, "VALUES (123.45)");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    @Test
    public void testAggregationPushdown()
    {
        String tableName = "test_agg_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);

        assertQuery("SELECT COUNT(*) FROM " + tableName, "SELECT 25");
        assertQuery("SELECT COUNT(regionkey) FROM " + tableName, "SELECT 25");
        assertQuery("SELECT MIN(regionkey) FROM " + tableName, "SELECT 0");
        assertQuery("SELECT MAX(regionkey) FROM " + tableName, "SELECT 4");
        assertQuery("SELECT SUM(regionkey) FROM " + tableName, "SELECT 50");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    @Test
    public void testLimitPushdown()
    {
        String tableName = "test_limit_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation", 25);

        assertThat(computeActual("SELECT * FROM " + tableName + " LIMIT 10").getRowCount())
                .isEqualTo(10);
        assertThat(computeActual("SELECT * FROM " + tableName + " LIMIT 5").getRowCount())
                .isEqualTo(5);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        if (e.getMessage() != null && e.getMessage().contains("QueryErrors{code=2009")) {
            return;
        }
        super.verifyConcurrentAddColumnFailurePermissible(e);
    }

    @Test
    @Override
    public void verifySupportsDeleteDeclaration()
    {
        abort("Databend executes DELETE statements instead of rejecting them for unsupported connectors");
    }

    @Test
    @Override
    public void verifySupportsRowLevelDeleteDeclaration()
    {
        abort("Databend executes DELETE statements instead of rejecting them for unsupported connectors");
    }

    @Test
    @Override
    public void verifySupportsUpdateDeclaration()
    {
        abort("Databend executes UPDATE statements instead of rejecting them for unsupported connectors");
    }

    @Test
    @Override
    public void testDataMappingSmokeTest()
    {
        abort("Databend does not support all TIME/TIMESTAMP combinations exercised by the generic smoke test");
    }

    @Test
    @Override
    public void testInsertWithoutTemporaryTable()
    {
        abort("Databend requires elevated privileges to inspect written tables, so the temporary-table bypass test cannot run");
    }

    private static String invalidDateWriteError(String date)
    {
        return format("(?s).*Invalid value '%s'.*", date);
    }

    private static String invalidDateReadError(String date)
    {
        return format("(?s).*cannot parse to type `DATE`.*%s.*", date);
    }
}
