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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.UNSUPPORTED_TYPE_HANDLING;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.redshift.RedshiftQueryRunner.TEST_SCHEMA;
import static io.trino.plugin.redshift.RedshiftQueryRunner.createRedshiftQueryRunner;
import static io.trino.plugin.redshift.RedshiftQueryRunner.executeInRedshift;
import static io.trino.plugin.redshift.RedshiftQueryRunner.executeWithRedshift;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.trueFalse;
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
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_COMMENT_ON_COLUMN,
                    SUPPORTS_JOIN_PUSHDOWN,
                    SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY -> true;
            case SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT,
                    SUPPORTS_ADD_COLUMN_WITH_COMMENT,
                    SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                    SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                    SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                    SUPPORTS_ARRAY,
                    SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                    SUPPORTS_DROP_SCHEMA_CASCADE,
                    SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM,
                    SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN,
                    SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                    SUPPORTS_ROW_TYPE,
                    SUPPORTS_SET_COLUMN_TYPE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    public void testSuperColumnType()
    {
        Session convertToVarchar = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), UNSUPPORTED_TYPE_HANDLING, CONVERT_TO_VARCHAR.name())
                .build();
        try (TestTable table = new TestTable(
                onRemoteDatabase(),
                format("%s.test_table_with_super_columns", TEST_SCHEMA),
                "(c1 integer, c2 super)",
                ImmutableList.of(
                        "1, null",
                        "2, 'super value string'",
                        "3, " + """
                                JSON_PARSE('{"r_nations":[
                                      {"n_comment":"s. ironic, unusual asymptotes wake blithely r",
                                         "n_nationkey":16,
                                         "n_name":"MOZAMBIQUE"
                                      }
                                   ]
                                }')
                                """,
                        "4, 4"))) {
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1), (2), (3), (4)");
            assertQuery(convertToVarchar, "SELECT * FROM " + table.getName(), """
                    VALUES
                    (1, null),
                    (2, '\"super value string\"'),
                    (3, '{"r_nations":[{"n_comment":"s. ironic, unusual asymptotes wake blithely r","n_nationkey":16,"n_name":"MOZAMBIQUE"}]}'),
                    (4, '4')
                    """);
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
            assertThat(query("SELECT true FROM %s WHERE value IS NULL".formatted(view.getName())))
                    .containsAll("VALUES (true)");

            assertThat(query("SHOW COLUMNS FROM %s LIKE 'value'".formatted(view.getName())))
                    .skippingTypesCheck()
                    .containsAll("VALUES ('value', '%s', '', '')".formatted(trinoType));
        }
    }

    @Test(dataProvider = "testReadNullFromViewDataProvider")
    public void testReadNullFromView(String redshiftType, String trinoType, boolean lateBindingView)
    {
        try (TestView view = new TestView(
                onRemoteDatabase(),
                TEST_SCHEMA + ".cast_null_view",
                "SELECT CAST(NULL AS %s) AS value %s".formatted(redshiftType, lateBindingView ? "WITH NO SCHEMA BINDING" : ""))) {
            assertThat(query("SELECT value FROM %s".formatted(view.getName())))
                    .skippingTypesCheck() // trino returns 'unknown' for null
                    .matches("VALUES null");

            assertThat(query("SHOW COLUMNS FROM %s LIKE 'value'".formatted(view.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES ('value', '%s', '', '')".formatted(trinoType));
        }
    }

    @DataProvider
    public Object[][] testReadNullFromViewDataProvider()
    {
        return cartesianProduct(redshiftTypeToTrinoTypes(), trueFalse());
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
                // consider to extract "CHARACTER VARYING" type from here as it requires exact length, 0 - is for the empty string
                {"CHARACTER VARYING", "varchar(0)"},
                {"TIME", "time(6)"},
                {"TIMESTAMP", "timestamp(6)"},
                {"TIMESTAMPTZ", "timestamp(6) with time zone"}};
    }

    @Test
    public void testRedshiftAddNotNullColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, TEST_SCHEMA + ".test_add_column_", "(col int)")) {
            assertThatThrownBy(() -> onRemoteDatabase().execute("ALTER TABLE " + table.getName() + " ADD COLUMN new_col int NOT NULL"))
                    .hasMessageContaining("ERROR: ALTER TABLE ADD COLUMN defined as NOT NULL must have a non-null default expression");
        }
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

    /**
     * Tries to create situation where Redshift would decide to materialize a temporary table for query sent to it by us.
     * Such temporary table requires that our Connection is not read-only.
     */
    @Test
    public void testComplexPushdownThatMayElicitTemporaryTable()
    {
        int subqueries = 10;
        String subquery = "SELECT custkey, count(*) c FROM orders GROUP BY custkey";
        StringBuilder sql = new StringBuilder();
        sql.append(format(
                "SELECT t0.custkey, %s c_sum ",
                IntStream.range(0, subqueries)
                        .mapToObj(i -> format("t%s.c", i))
                        .collect(Collectors.joining("+"))));
        sql.append(format("FROM (%s) t0 ", subquery));
        for (int i = 1; i < subqueries; i++) {
            sql.append(format("JOIN (%s) t%s ON t0.custkey = t%s.custkey ", subquery, i, i));
        }
        sql.append("WHERE t0.custkey = 1045 OR rand() = 42");

        Session forceJoinPushdown = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();

        assertThat(query(forceJoinPushdown, sql.toString()))
                .matches(format("SELECT max(custkey), count(*) * %s FROM tpch.tiny.orders WHERE custkey = 1045", subqueries));
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
    public void testCountDistinctWithStringTypes()
    {
        // cannot test using generic method as Redshift does not allow non-ASCII characters in CHAR values.
        assertThatThrownBy(super::testCountDistinctWithStringTypes).hasMessageContaining("Value for Redshift CHAR must be ASCII, but found 'ą'");

        List<String> rows = Stream.of("a", "b", "A", "B", " a ", "a", "b", " b ")
                .map(value -> format("'%1$s', '%1$s'", value))
                .collect(toImmutableList());
        String tableName = "distinct_strings" + randomNameSuffix();

        try (TestTable testTable = new TestTable(getQueryRunner()::execute, tableName, "(t_char CHAR(5), t_varchar VARCHAR(5))", rows)) {
            // Single count(DISTINCT ...) can be pushed even down even if SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT == false as GROUP BY
            assertThat(query("SELECT count(DISTINCT t_varchar) FROM " + testTable.getName()))
                    .matches("VALUES BIGINT '6'")
                    .isFullyPushedDown();

            // Single count(DISTINCT ...) can be pushed down even if SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT == false as GROUP BY
            assertThat(query("SELECT count(DISTINCT t_char) FROM " + testTable.getName()))
                    .matches("VALUES BIGINT '6'")
                    .isFullyPushedDown();

            assertThat(query("SELECT count(DISTINCT t_char), count(DISTINCT t_varchar) FROM " + testTable.getName()))
                    .matches("VALUES (BIGINT '6', BIGINT '6')")
                    .isFullyPushedDown();
        }
    }

    @Override
    public void testAggregationPushdown()
    {
        throw new SkipException("tested in testAggregationPushdown(String)");
    }

    @Test(dataProvider = "testAggregationPushdownDistStylesDataProvider")
    public void testAggregationPushdown(String distStyle)
    {
        String nation = format("%s.nation_%s_%s", TEST_SCHEMA, distStyle, randomNameSuffix());
        String customer = format("%s.customer_%s_%s", TEST_SCHEMA, distStyle, randomNameSuffix());
        try {
            copyWithDistStyle(TEST_SCHEMA + ".nation", nation, distStyle, Optional.of("regionkey"));
            copyWithDistStyle(TEST_SCHEMA + ".customer", customer, distStyle, Optional.of("nationkey"));

            // TODO support aggregation pushdown with GROUPING SETS
            // TODO support aggregation over expressions

            // count()
            assertThat(query("SELECT count(*) FROM " + nation)).isFullyPushedDown();
            assertThat(query("SELECT count(nationkey) FROM " + nation)).isFullyPushedDown();
            assertThat(query("SELECT count(1) FROM " + nation)).isFullyPushedDown();
            assertThat(query("SELECT count() FROM " + nation)).isFullyPushedDown();
            assertThat(query("SELECT regionkey, count(1) FROM " + nation + " GROUP BY regionkey")).isFullyPushedDown();
            try (TestTable emptyTable = createAggregationTestTable(getSession().getSchema().orElseThrow() + ".empty_table", ImmutableList.of())) {
                String emptyTableName = emptyTable.getName() + "_" + distStyle;
                copyWithDistStyle(emptyTable.getName(), emptyTableName, distStyle, Optional.of("a_bigint"));

                assertThat(query("SELECT count(*) FROM " + emptyTableName)).isFullyPushedDown();
                assertThat(query("SELECT count(a_bigint) FROM " + emptyTableName)).isFullyPushedDown();
                assertThat(query("SELECT count(1) FROM " + emptyTableName)).isFullyPushedDown();
                assertThat(query("SELECT count() FROM " + emptyTableName)).isFullyPushedDown();
                assertThat(query("SELECT a_bigint, count(1) FROM " + emptyTableName + " GROUP BY a_bigint")).isFullyPushedDown();
            }

            // GROUP BY
            assertThat(query("SELECT regionkey, min(nationkey) FROM " + nation + " GROUP BY regionkey")).isFullyPushedDown();
            assertThat(query("SELECT regionkey, max(nationkey) FROM " + nation + " GROUP BY regionkey")).isFullyPushedDown();
            assertThat(query("SELECT regionkey, sum(nationkey) FROM " + nation + " GROUP BY regionkey")).isFullyPushedDown();
            assertThat(query("SELECT regionkey, avg(nationkey) FROM " + nation + " GROUP BY regionkey")).isFullyPushedDown();
            try (TestTable emptyTable = createAggregationTestTable(getSession().getSchema().orElseThrow() + ".empty_table", ImmutableList.of())) {
                String emptyTableName = emptyTable.getName() + "_" + distStyle;
                copyWithDistStyle(emptyTable.getName(), emptyTableName, distStyle, Optional.of("a_bigint"));

                assertThat(query("SELECT t_double, min(a_bigint) FROM " + emptyTableName + " GROUP BY t_double")).isFullyPushedDown();
                assertThat(query("SELECT t_double, max(a_bigint) FROM " + emptyTableName + " GROUP BY t_double")).isFullyPushedDown();
                assertThat(query("SELECT t_double, sum(a_bigint) FROM " + emptyTableName + " GROUP BY t_double")).isFullyPushedDown();
                assertThat(query("SELECT t_double, avg(a_bigint) FROM " + emptyTableName + " GROUP BY t_double")).isFullyPushedDown();
            }

            // GROUP BY and WHERE on bigint column
            // GROUP BY and WHERE on aggregation key
            assertThat(query("SELECT regionkey, sum(nationkey) FROM " + nation + " WHERE regionkey < 4 GROUP BY regionkey")).isFullyPushedDown();

            // GROUP BY and WHERE on varchar column
            // GROUP BY and WHERE on "other" (not aggregation key, not aggregation input)
            assertThat(query("SELECT regionkey, sum(nationkey) FROM " + nation + " WHERE regionkey < 4 AND name > 'AAA' GROUP BY regionkey")).isFullyPushedDown();
            // GROUP BY above WHERE and LIMIT
            assertThat(query("SELECT regionkey, sum(nationkey) FROM (SELECT * FROM " + nation + " WHERE regionkey < 2 LIMIT 11) GROUP BY regionkey")).isFullyPushedDown();
            // GROUP BY above TopN
            assertThat(query("SELECT regionkey, sum(nationkey) FROM (SELECT regionkey, nationkey FROM " + nation + " ORDER BY nationkey ASC LIMIT 10) GROUP BY regionkey")).isFullyPushedDown();
            // GROUP BY with JOIN
            assertThat(query(
                    joinPushdownEnabled(getSession()),
                    "SELECT n.regionkey, sum(c.acctbal) acctbals FROM " + nation + " n LEFT JOIN " + customer + " c USING (nationkey) GROUP BY 1"))
                    .isFullyPushedDown();
            // GROUP BY with WHERE on neither grouping nor aggregation column
            assertThat(query("SELECT nationkey, min(regionkey) FROM " + nation + " WHERE name = 'ARGENTINA' GROUP BY nationkey")).isFullyPushedDown();
            // aggregation on varchar column
            assertThat(query("SELECT count(name) FROM " + nation)).isFullyPushedDown();
            // aggregation on varchar column with GROUPING
            assertThat(query("SELECT nationkey, count(name) FROM " + nation + " GROUP BY nationkey")).isFullyPushedDown();
            // aggregation on varchar column with WHERE
            assertThat(query("SELECT count(name) FROM " + nation + " WHERE name = 'ARGENTINA'")).isFullyPushedDown();
        }
        finally {
            executeInRedshift("DROP TABLE IF EXISTS " + nation);
            executeInRedshift("DROP TABLE IF EXISTS " + customer);
        }
    }

    @Override
    public void testNumericAggregationPushdown()
    {
        throw new SkipException("tested in testNumericAggregationPushdown(String)");
    }

    @Test(dataProvider = "testAggregationPushdownDistStylesDataProvider")
    public void testNumericAggregationPushdown(String distStyle)
    {
        String schemaName = getSession().getSchema().orElseThrow();
        // empty table
        try (TestTable emptyTable = createAggregationTestTable(schemaName + ".test_aggregation_pushdown", ImmutableList.of())) {
            String emptyTableName = emptyTable.getName() + "_" + distStyle;
            copyWithDistStyle(emptyTable.getName(), emptyTableName, distStyle, Optional.of("a_bigint"));

            assertThat(query("SELECT min(short_decimal), min(long_decimal), min(a_bigint), min(t_double) FROM " + emptyTableName)).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal), max(a_bigint), max(t_double) FROM " + emptyTableName)).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal), sum(a_bigint), sum(t_double) FROM " + emptyTableName)).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal), avg(a_bigint), avg(t_double) FROM " + emptyTableName)).isFullyPushedDown();
        }

        try (TestTable testTable = createAggregationTestTable(schemaName + ".test_aggregation_pushdown",
                ImmutableList.of("100.000, 100000000.000000000, 100.000, 100000000", "123.321, 123456789.987654321, 123.321, 123456789"))) {
            String testTableName = testTable.getName() + "_" + distStyle;
            copyWithDistStyle(testTable.getName(), testTableName, distStyle, Optional.of("a_bigint"));

            assertThat(query("SELECT min(short_decimal), min(long_decimal), min(a_bigint), min(t_double) FROM " + testTableName)).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal), max(a_bigint), max(t_double) FROM " + testTableName)).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal), sum(a_bigint), sum(t_double) FROM " + testTableName)).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal), avg(a_bigint), avg(t_double) FROM " + testTableName)).isFullyPushedDown();

            // smoke testing of more complex cases
            // WHERE on aggregation column
            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM " + testTableName + " WHERE short_decimal < 110 AND long_decimal < 124")).isFullyPushedDown();
            // WHERE on non-aggregation column
            assertThat(query("SELECT min(long_decimal) FROM " + testTableName + " WHERE short_decimal < 110")).isFullyPushedDown();
            // GROUP BY
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTableName + " GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on both grouping and aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTableName + " WHERE short_decimal < 110 AND long_decimal < 124 GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on grouping column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTableName + " WHERE short_decimal < 110 GROUP BY short_decimal")).isFullyPushedDown();
            // GROUP BY with WHERE on aggregation column
            assertThat(query("SELECT short_decimal, min(long_decimal) FROM " + testTableName + " WHERE long_decimal < 124 GROUP BY short_decimal")).isFullyPushedDown();
        }
    }

    private static void copyWithDistStyle(String sourceTableName, String destTableName, String distStyle, Optional<String> distKey)
    {
        if (distStyle.equals("AUTO")) {
            // NOTE: Redshift doesn't support setting diststyle AUTO in CTAS statements
            executeInRedshift("CREATE TABLE " + destTableName + " AS SELECT * FROM " + sourceTableName);
            // Redshift doesn't allow ALTER DISTSTYLE if original and new style are same, so we need to check current diststyle of table
            boolean isDistStyleAuto = executeWithRedshift(handle -> {
                Optional<Long> currentDistStyle = handle.createQuery("" +
                                "SELECT releffectivediststyle " +
                                "FROM pg_class_info AS a LEFT JOIN pg_namespace AS b ON a.relnamespace = b.oid " +
                                "WHERE lower(nspname) = lower(:schema_name) AND lower(relname) = lower(:table_name)")
                        .bind("schema_name", TEST_SCHEMA)
                        // destTableName = TEST_SCHEMA + "." + tableName
                        .bind("table_name", destTableName.substring(destTableName.indexOf(".") + 1))
                        .mapTo(Long.class)
                        .findOne();

                // 10 means AUTO(ALL) and 11 means AUTO(EVEN). See https://docs.aws.amazon.com/redshift/latest/dg/r_PG_CLASS_INFO.html.
                return currentDistStyle.isPresent() && (currentDistStyle.get() == 10 || currentDistStyle.get() == 11);
            });
            if (!isDistStyleAuto) {
                executeInRedshift("ALTER TABLE " + destTableName + " ALTER DISTSTYLE " + distStyle);
            }
        }
        else {
            String copyWithDistStyleSql = "CREATE TABLE " + destTableName + " DISTSTYLE " + distStyle;
            if (distStyle.equals("KEY")) {
                copyWithDistStyleSql += format(" DISTKEY(%s)", distKey.orElseThrow());
            }
            copyWithDistStyleSql += " AS SELECT * FROM " + sourceTableName;
            executeInRedshift(copyWithDistStyleSql);
        }
    }

    @DataProvider
    public Object[][] testAggregationPushdownDistStylesDataProvider()
    {
        return new Object[][] {
                {"EVEN"},
                {"KEY"},
                {"ALL"},
                {"AUTO"},
        };
    }

    @Test
    public void testDecimalAvgPushdownForMaximumDecimalScale()
    {
        List<String> rows = ImmutableList.of(
                "12345789.9876543210",
                format("%s.%s", "1".repeat(28), "9".repeat(10)));

        try (TestTable testTable = new TestTable(getQueryRunner()::execute, TEST_SCHEMA + ".test_agg_pushdown_avg_max_decimal",
                "(t_decimal DECIMAL(38, 10))", rows)) {
            // Redshift avg rounds down decimal result which doesn't match Presto semantics
            assertThatThrownBy(() -> assertThat(query("SELECT avg(t_decimal) FROM " + testTable.getName())).isFullyPushedDown())
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("""
                            elements not found:
                              (555555555555555555561728450.9938271605)
                            and elements not expected:
                              (555555555555555555561728450.9938271604)
                            """);
        }
    }

    @Test
    public void testDecimalAvgPushdownFoShortDecimalScale()
    {
        List<String> rows = ImmutableList.of(
                "0.987654321234567890",
                format("0.%s", "1".repeat(18)));

        try (TestTable testTable = new TestTable(getQueryRunner()::execute, TEST_SCHEMA + ".test_agg_pushdown_avg_max_decimal",
                "(t_decimal DECIMAL(18, 18))", rows)) {
            assertThat(query("SELECT avg(t_decimal) FROM " + testTable.getName())).isFullyPushedDown();
        }
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
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
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
