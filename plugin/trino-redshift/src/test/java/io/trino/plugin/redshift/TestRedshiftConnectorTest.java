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
package io.trino.plugin.redshift;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.plugin.redshift.RedshiftQueryRunner.TEST_SCHEMA;
import static io.trino.plugin.redshift.RedshiftQueryRunner.createRedshiftQueryRunner;
import static io.trino.plugin.redshift.RedshiftQueryRunner.executeInRedshift;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRedshiftConnectorTest
        extends BaseJdbcConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createRedshiftQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of(),
                // NOTE this can cause tests to time-out if larger tables like
                //  lineitem and orders need to be re-created.
                TpchTable.getTables());
    }

    @Override
    @SuppressWarnings("DuplicateBranchesInSwitch") // options here are grouped per-feature
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN:
            case SUPPORTS_JOIN_PUSHDOWN:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_ADD_COLUMN_WITH_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT:
            case SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT:
                return false;

            case SUPPORTS_ARRAY:
            case SUPPORTS_ROW_TYPE:
                return false;

            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                format("%s.test_table_with_default_columns", TEST_SCHEMA),
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if ("date".equals(typeName)) {
            if (dataMappingTestSetup.getSampleValueLiteral().equals("DATE '1582-10-05'")) {
                return Optional.empty();
            }
        }
        return Optional.of(dataMappingTestSetup);
    }

    /**
     * Overridden due to Redshift not supporting non-ASCII characters in CHAR.
     */
    @Override
    public void testCreateTableAsSelectWithUnicode()
    {
        assertThatThrownBy(super::testCreateTableAsSelectWithUnicode)
                .hasStackTraceContaining("Value too long for character type");
        // NOTE we add a copy of the above using VARCHAR which supports non-ASCII characters
        assertCreateTableAsSelect(
                "SELECT CAST('\u2603' AS VARCHAR) unicode",
                "SELECT 1");
    }

    @Test(dataProvider = "redshiftTypeToTrinoTypes")
    public void testReadFromLateBindingView(String redshiftType, String trinoType)
    {
        try (TestView view = new TestView(onRemoteDatabase(), TEST_SCHEMA + ".late_schema_binding", "SELECT CAST(NULL AS %s) AS value WITH NO SCHEMA BINDING".formatted(redshiftType))) {
            assertThat(query("SELECT value, true FROM %s WHERE value IS NULL".formatted(view.getName())))
                    .projected(1)
                    .containsAll("VALUES (true)");

            assertThat(query("SHOW COLUMNS FROM %s LIKE 'value'".formatted(view.getName())))
                    .projected(1)
                    .skippingTypesCheck()
                    .containsAll("VALUES ('%s')".formatted(trinoType));
        }
    }

    @DataProvider
    public Object[][] redshiftTypeToTrinoTypes()
    {
        return new Object[][] {
                {"SMALLINT", "smallint"},
                {"INTEGER", "integer"},
                {"BIGINT", "bigint"},
                {"DECIMAL", "decimal(18,0)"},
                {"REAL", "real"},
                {"DOUBLE PRECISION", "double"},
                {"BOOLEAN", "boolean"},
                {"CHAR(1)", "char(1)"},
                {"VARCHAR(1)", "varchar(1)"},
                {"TIME", "time(6)"},
                {"TIMESTAMP", "timestamp(6)"},
                {"TIMESTAMPTZ", "timestamp(6) with time zone"}};
    }

    @Override
    public void testDelete()
    {
        // The base tests is very slow because Redshift CTAS is really slow, so use a smaller test
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_", "AS SELECT * FROM nation")) {
            // delete without matching any rows
            assertUpdate("DELETE FROM " + table.getName() + " WHERE nationkey < 0", 0);

            // delete with a predicate that optimizes to false
            assertUpdate("DELETE FROM " + table.getName() + " WHERE nationkey > 5 AND nationkey < 4", 0);

            // delete successive parts of the table
            assertUpdate("DELETE FROM " + table.getName() + " WHERE nationkey <= 5", "SELECT count(*) FROM nation WHERE nationkey <= 5");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM nation WHERE nationkey > 5");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE nationkey <= 10", "SELECT count(*) FROM nation WHERE nationkey > 5 AND nationkey <= 10");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM nation WHERE nationkey > 10");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE nationkey <= 15", "SELECT count(*) FROM nation WHERE nationkey > 10 AND nationkey <= 15");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM nation WHERE nationkey > 15");

            // delete remaining
            assertUpdate("DELETE FROM " + table.getName(), "SELECT count(*) FROM nation WHERE nationkey > 15");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM nation WHERE false");
        }
    }

    @Test(dataProvider = "testCaseColumnNamesDataProvider")
    public void testCaseColumnNames(String tableName)
    {
        try {
            assertUpdate(
                    "CREATE TABLE " + TEST_SCHEMA + "." + tableName +
                            " AS SELECT " +
                            "  custkey AS CASE_UNQUOTED_UPPER, " +
                            "  name AS case_unquoted_lower, " +
                            "  address AS cASe_uNQuoTeD_miXED, " +
                            "  nationkey AS \"CASE_QUOTED_UPPER\", " +
                            "  phone AS \"case_quoted_lower\"," +
                            "  acctbal AS \"CasE_QuoTeD_miXED\" " +
                            "FROM customer",
                    1500);
            gatherStats(tableName);
            assertQuery(
                    "SHOW STATS FOR " + TEST_SCHEMA + "." + tableName,
                    "VALUES " +
                            "('case_unquoted_upper', NULL, 1485, 0, null, null, null)," +
                            "('case_unquoted_lower', 33000, 1470, 0, null, null, null)," +
                            "('case_unquoted_mixed', 42000, 1500, 0, null, null, null)," +
                            "('case_quoted_upper', NULL, 25, 0, null, null, null)," +
                            "('case_quoted_lower', 28500, 1483, 0, null, null, null)," +
                            "('case_quoted_mixed', NULL, 1483, 0, null, null, null)," +
                            "(null, null, null, null, 1500, null, null)");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private static void gatherStats(String tableName)
    {
        executeInRedshift(handle -> {
            handle.execute("ANALYZE VERBOSE " + TEST_SCHEMA + "." + tableName);
            for (int i = 0; i < 5; i++) {
                long actualCount = handle.createQuery("SELECT count(*) FROM " + TEST_SCHEMA + "." + tableName)
                        .mapTo(Long.class)
                        .one();
                long estimatedCount = handle.createQuery("""
                                SELECT reltuples FROM pg_class
                                WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = :schema)
                                AND relname = :table_name
                                """)
                        .bind("schema", TEST_SCHEMA)
                        .bind("table_name", tableName.toLowerCase(ENGLISH).replace("\"", ""))
                        .mapTo(Long.class)
                        .one();
                if (actualCount == estimatedCount) {
                    return;
                }
                handle.execute("ANALYZE VERBOSE " + TEST_SCHEMA + "." + tableName);
            }
            throw new IllegalStateException("Stats not gathered"); // for small test tables reltuples should be exact
        });
    }

    @DataProvider
    public Object[][] testCaseColumnNamesDataProvider()
    {
        return new Object[][] {
                {"TEST_STATS_MIXED_UNQUOTED_UPPER_" + randomNameSuffix()},
                {"test_stats_mixed_unquoted_lower_" + randomNameSuffix()},
                {"test_stats_mixed_uNQuoTeD_miXED_" + randomNameSuffix()},
                {"\"TEST_STATS_MIXED_QUOTED_UPPER_" + randomNameSuffix() + "\""},
                {"\"test_stats_mixed_quoted_lower_" + randomNameSuffix() + "\""},
                {"\"test_stats_mixed_QuoTeD_miXED_" + randomNameSuffix() + "\""}
        };
    }

    @Override
    @Test
    public void testReadMetadataWithRelationsConcurrentModifications()
    {
        throw new SkipException("Test fails with a timeout sometimes and is flaky");
    }

    @Override
    public void testInsertRowConcurrently()
    {
        throw new SkipException("Test fails with a timeout sometimes and is flaky");
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("(?s).*Cannot insert a NULL value into column %s.*", columnName);
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(127);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessage("Schema name must be shorter than or equal to '127' characters but got '128'");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(127);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessage("Table name must be shorter than or equal to '127' characters but got '128'");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(127);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessage("Column name must be shorter than or equal to '127' characters but got '128'");
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return RedshiftQueryRunner::executeInRedshift;
    }

    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: This connector does not support modifying table rows");
    }

    @Test
    @Override
    public void testAddNotNullColumnToNonEmptyTable()
    {
        throw new SkipException("Redshift ALTER TABLE ADD COLUMN defined as NOT NULL must have a non-null default expression");
    }

    private static class TestView
            implements AutoCloseable
    {
        private final String name;
        private final SqlExecutor executor;

        public TestView(SqlExecutor executor, String namePrefix, String viewDefinition)
        {
            this.executor = executor;
            this.name = namePrefix + "_" + randomNameSuffix();
            executor.execute("CREATE OR REPLACE VIEW " + name + " AS " + viewDefinition);
        }

        @Override
        public void close()
        {
            executor.execute("DROP VIEW " + name);
        }

        public String getName()
        {
            return name;
        }
    }
}
