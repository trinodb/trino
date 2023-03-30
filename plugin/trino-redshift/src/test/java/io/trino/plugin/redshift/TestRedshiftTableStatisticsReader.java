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

import com.amazon.redshift.Driver;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.credential.StaticCredentialProvider;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static io.trino.plugin.redshift.RedshiftQueryRunner.JDBC_PASSWORD;
import static io.trino.plugin.redshift.RedshiftQueryRunner.JDBC_URL;
import static io.trino.plugin.redshift.RedshiftQueryRunner.JDBC_USER;
import static io.trino.plugin.redshift.RedshiftQueryRunner.TEST_SCHEMA;
import static io.trino.plugin.redshift.RedshiftQueryRunner.createRedshiftQueryRunner;
import static io.trino.plugin.redshift.RedshiftQueryRunner.executeInRedshift;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.sql.TestTable.fromColumns;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.from;
import static org.assertj.core.api.Assertions.withinPercentage;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestRedshiftTableStatisticsReader
        extends AbstractTestQueryFramework
{
    private static final JdbcTypeHandle BIGINT_TYPE_HANDLE = new JdbcTypeHandle(Types.BIGINT, Optional.of("int8"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    private static final JdbcTypeHandle DOUBLE_TYPE_HANDLE = new JdbcTypeHandle(Types.DOUBLE, Optional.of("double"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    private static final List<JdbcColumnHandle> CUSTOMER_COLUMNS = ImmutableList.of(
            new JdbcColumnHandle("custkey", BIGINT_TYPE_HANDLE, BIGINT),
            createVarcharJdbcColumnHandle("name", 25),
            createVarcharJdbcColumnHandle("address", 48),
            new JdbcColumnHandle("nationkey", BIGINT_TYPE_HANDLE, BIGINT),
            createVarcharJdbcColumnHandle("phone", 15),
            new JdbcColumnHandle("acctbal", DOUBLE_TYPE_HANDLE, DOUBLE),
            createVarcharJdbcColumnHandle("mktsegment", 10),
            createVarcharJdbcColumnHandle("comment", 117));

    private RedshiftTableStatisticsReader statsReader;

    @BeforeClass
    public void setup()
    {
        DriverConnectionFactory connectionFactory = new DriverConnectionFactory(
                new Driver(),
                new BaseJdbcConfig().setConnectionUrl(JDBC_URL),
                new StaticCredentialProvider(Optional.of(JDBC_USER), Optional.of(JDBC_PASSWORD)));
        statsReader = new RedshiftTableStatisticsReader(connectionFactory);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createRedshiftQueryRunner(Map.of(), Map.of(), ImmutableList.of(CUSTOMER));
    }

    @Test
    public void testCustomerTable()
            throws Exception
    {
        assertThat(collectStats("SELECT * FROM " + TEST_SCHEMA + ".customer", CUSTOMER_COLUMNS))
                .returns(Estimate.of(1500), from(TableStatistics::getRowCount))
                .extracting(TableStatistics::getColumnStatistics, InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class))
                .hasEntrySatisfying(CUSTOMER_COLUMNS.get(0), statsCloseTo(1500.0, 0.0, 8.0 * 1500))
                .hasEntrySatisfying(CUSTOMER_COLUMNS.get(1), statsCloseTo(1500.0, 0.0, 33000.0))
                .hasEntrySatisfying(CUSTOMER_COLUMNS.get(3), statsCloseTo(25.000, 0.0, 8.0 * 1500))
                .hasEntrySatisfying(CUSTOMER_COLUMNS.get(5), statsCloseTo(1499.0, 0.0, 8.0 * 1500));
    }

    @Test
    public void testEmptyTable()
            throws Exception
    {
        TableStatistics tableStatistics = collectStats("SELECT * FROM " + TEST_SCHEMA + ".customer WHERE false", CUSTOMER_COLUMNS);
        assertThat(tableStatistics)
                .returns(Estimate.of(0.0), from(TableStatistics::getRowCount))
                .returns(emptyMap(), from(TableStatistics::getColumnStatistics));
    }

    @Test
    public void testAllNulls()
            throws Exception
    {
        String tableName = "testallnulls_" + randomNameSuffix();
        String schemaAndTable = TEST_SCHEMA + "." + tableName;
        try {
            executeInRedshift("CREATE TABLE " + schemaAndTable + " (i BIGINT)");
            executeInRedshift("INSERT INTO " + schemaAndTable + " (i) VALUES (NULL)");
            executeInRedshift("ANALYZE VERBOSE " + schemaAndTable);

            TableStatistics stats = statsReader.readTableStatistics(
                    SESSION,
                    new JdbcTableHandle(
                            new SchemaTableName(TEST_SCHEMA, tableName),
                            new RemoteTableName(Optional.empty(), Optional.of(TEST_SCHEMA), tableName),
                            Optional.empty()),
                    () -> ImmutableList.of(new JdbcColumnHandle("i", BIGINT_TYPE_HANDLE, BIGINT)));
            assertThat(stats)
                    .returns(Estimate.of(1.0), from(TableStatistics::getRowCount))
                    .returns(emptyMap(), from(TableStatistics::getColumnStatistics));
        }
        finally {
            executeInRedshift("DROP TABLE IF EXISTS " + schemaAndTable);
        }
    }

    @Test
    public void testNullsFraction()
            throws Exception
    {
        JdbcColumnHandle custkeyColumnHandle = CUSTOMER_COLUMNS.get(0);
        TableStatistics stats = collectStats(
                "SELECT CASE custkey % 3 WHEN 0 THEN NULL ELSE custkey END FROM " + TEST_SCHEMA + ".customer",
                ImmutableList.of(custkeyColumnHandle));
        assertEquals(stats.getRowCount(), Estimate.of(1500));

        ColumnStatistics columnStatistics = stats.getColumnStatistics().get(custkeyColumnHandle);
        assertThat(columnStatistics.getNullsFraction().getValue()).isCloseTo(1.0 / 3, withinPercentage(1));
    }

    @Test
    public void testAverageColumnLength()
            throws Exception
    {
        List<JdbcColumnHandle> columns = ImmutableList.of(
                new JdbcColumnHandle("custkey", BIGINT_TYPE_HANDLE, BIGINT),
                createVarcharJdbcColumnHandle("v3_in_3", 3),
                createVarcharJdbcColumnHandle("v3_in_42", 42),
                createVarcharJdbcColumnHandle("single_10v_value", 10),
                createVarcharJdbcColumnHandle("half_10v_value", 10),
                createVarcharJdbcColumnHandle("half_distinct_20v_value", 20),
                createVarcharJdbcColumnHandle("all_nulls", 10));

        assertThat(
                collectStats(
                        "SELECT " +
                                "  custkey, " +
                                "  'abc' v3_in_3, " +
                                "  CAST('abc' AS varchar(42)) v3_in_42, " +
                                "  CASE custkey WHEN 1 THEN '0123456789' ELSE NULL END single_10v_value, " +
                                "  CASE custkey % 2 WHEN 0 THEN '0123456789' ELSE NULL END half_10v_value, " +
                                "  CASE custkey % 2 WHEN 0 THEN CAST((1000000 - custkey) * (1000000 - custkey) AS varchar(20)) ELSE NULL END half_distinct_20v_value, " + // 12 chars each
                                "  CAST(NULL AS varchar(10)) all_nulls " +
                                "FROM  " + TEST_SCHEMA + ".customer " +
                                "ORDER BY custkey LIMIT 100",
                        columns))
                .returns(Estimate.of(100), from(TableStatistics::getRowCount))
                .extracting(TableStatistics::getColumnStatistics, InstanceOfAssertFactories.map(ColumnHandle.class, ColumnStatistics.class))
                .hasEntrySatisfying(columns.get(0), statsCloseTo(100.0, 0.0, 800))
                .hasEntrySatisfying(columns.get(1), statsCloseTo(1.0, 0.0, 700.0))
                .hasEntrySatisfying(columns.get(2), statsCloseTo(1.0, 0.0, 700))
                .hasEntrySatisfying(columns.get(3), statsCloseTo(1.0, 0.99, 14))
                .hasEntrySatisfying(columns.get(4), statsCloseTo(1.0, 0.5, 700))
                .hasEntrySatisfying(columns.get(5), statsCloseTo(51, 0.5, 800))
                .satisfies(stats -> assertNull(stats.get(columns.get(6))));
    }

    @Test
    public void testView()
            throws Exception
    {
        String tableName = "test_stats_view_" + randomNameSuffix();
        String schemaAndTable = TEST_SCHEMA + "." + tableName;
        List<JdbcColumnHandle> columns = ImmutableList.of(
                new JdbcColumnHandle("custkey", BIGINT_TYPE_HANDLE, BIGINT),
                createVarcharJdbcColumnHandle("mktsegment", 10),
                createVarcharJdbcColumnHandle("comment", 117));

        try {
            executeInRedshift("CREATE OR REPLACE VIEW " + schemaAndTable + " AS SELECT custkey, mktsegment, comment FROM " + TEST_SCHEMA + ".customer");
            TableStatistics tableStatistics = statsReader.readTableStatistics(
                    SESSION,
                    new JdbcTableHandle(
                            new SchemaTableName(TEST_SCHEMA, tableName),
                            new RemoteTableName(Optional.empty(), Optional.of(TEST_SCHEMA), tableName),
                            Optional.empty()),
                    () -> columns);
            assertThat(tableStatistics).isEqualTo(TableStatistics.empty());
        }
        finally {
            executeInRedshift("DROP VIEW IF EXISTS " + schemaAndTable);
        }
    }

    @Test
    public void testMaterializedView()
            throws Exception
    {
        String tableName = "test_stats_materialized_view_" + randomNameSuffix();
        String schemaAndTable = TEST_SCHEMA + "." + tableName;
        List<JdbcColumnHandle> columns = ImmutableList.of(
                new JdbcColumnHandle("custkey", BIGINT_TYPE_HANDLE, BIGINT),
                createVarcharJdbcColumnHandle("mktsegment", 10),
                createVarcharJdbcColumnHandle("comment", 117));

        try {
            executeInRedshift("CREATE MATERIALIZED VIEW " + schemaAndTable +
                    " AS SELECT custkey, mktsegment, comment FROM " + TEST_SCHEMA + ".customer");
            executeInRedshift("REFRESH MATERIALIZED VIEW " + schemaAndTable);
            executeInRedshift("ANALYZE VERBOSE " + schemaAndTable);
            TableStatistics tableStatistics = statsReader.readTableStatistics(
                    SESSION,
                    new JdbcTableHandle(
                            new SchemaTableName(TEST_SCHEMA, tableName),
                            new RemoteTableName(Optional.empty(), Optional.of(TEST_SCHEMA), tableName),
                            Optional.empty()),
                    () -> columns);
            assertThat(tableStatistics).isEqualTo(TableStatistics.empty());
        }
        finally {
            executeInRedshift("DROP MATERIALIZED VIEW " + schemaAndTable);
        }
    }

    @Test
    public void testNumericCornerCases()
    {
        try (TestTable table = fromColumns(
                getQueryRunner()::execute,
                "test_numeric_corner_cases_",
                ImmutableMap.<String, List<String>>builder()
                        .put("only_negative_infinity double", List.of("-infinity()", "-infinity()", "-infinity()", "-infinity()"))
                        .put("only_positive_infinity double", List.of("infinity()", "infinity()", "infinity()", "infinity()"))
                        .put("mixed_infinities double", List.of("-infinity()", "infinity()", "-infinity()", "infinity()"))
                        .put("mixed_infinities_and_numbers double", List.of("-infinity()", "infinity()", "-5.0", "7.0"))
                        .put("nans_only double", List.of("nan()", "nan()"))
                        .put("nans_and_numbers double", List.of("nan()", "nan()", "-5.0", "7.0"))
                        .put("large_doubles double", List.of("CAST(-50371909150609548946090.0 AS DOUBLE)", "CAST(50371909150609548946090.0 AS DOUBLE)")) // 2^77 DIV 3
                        .put("short_decimals_big_fraction decimal(16,15)", List.of("-1.234567890123456", "1.234567890123456"))
                        .put("short_decimals_big_integral decimal(16,1)", List.of("-123456789012345.6", "123456789012345.6"))
                        .put("long_decimals_big_fraction decimal(38,37)", List.of("-1.2345678901234567890123456789012345678", "1.2345678901234567890123456789012345678"))
                        .put("long_decimals_middle decimal(38,16)", List.of("-1234567890123456.7890123456789012345678", "1234567890123456.7890123456789012345678"))
                        .put("long_decimals_big_integral decimal(38,1)", List.of("-1234567890123456789012345678901234567.8", "1234567890123456789012345678901234567.8"))
                        .buildOrThrow(),
                "null")) {
            executeInRedshift("ANALYZE VERBOSE " + TEST_SCHEMA + "." + table.getName());
            assertQuery(
                    "SHOW STATS FOR " + table.getName(),
                    "VALUES " +
                            "('only_negative_infinity', null, 1, 0, null, null, null)," +
                            "('only_positive_infinity', null, 1, 0, null, null, null)," +
                            "('mixed_infinities', null, 2, 0, null, null, null)," +
                            "('mixed_infinities_and_numbers', null, 4.0, 0.0, null, null, null)," +
                            "('nans_only', null, 1.0, 0.5, null, null, null)," +
                            "('nans_and_numbers', null, 3.0, 0.0, null, null, null)," +
                            "('large_doubles', null, 2.0, 0.5, null, null, null)," +
                            "('short_decimals_big_fraction', null, 2.0, 0.5, null, null, null)," +
                            "('short_decimals_big_integral', null, 2.0, 0.5, null, null, null)," +
                            "('long_decimals_big_fraction', null, 2.0, 0.5, null, null, null)," +
                            "('long_decimals_middle', null, 2.0, 0.5, null, null, null)," +
                            "('long_decimals_big_integral', null, 2.0, 0.5, null, null, null)," +
                            "(null, null, null, null, 4, null, null)");
        }
    }

    /**
     * Assert that the given column is within 5% of each statistic in the parameters, and that it has no range
     */
    private static Consumer<ColumnStatistics> statsCloseTo(double distinctValues, double nullsFraction, double dataSize)
    {
        return stats -> {
            SoftAssertions softly = new SoftAssertions();

            softly.assertThat(stats.getDistinctValuesCount().getValue())
                    .isCloseTo(distinctValues, withinPercentage(5.0));

            softly.assertThat(stats.getNullsFraction().getValue())
                    .isCloseTo(nullsFraction, withinPercentage(5.0));

            softly.assertThat(stats.getDataSize().getValue())
                    .isCloseTo(dataSize, withinPercentage(5.0));

            softly.assertThat(stats.getRange()).isEmpty();
            softly.assertAll();
        };
    }

    private TableStatistics collectStats(String values, List<JdbcColumnHandle> columnHandles)
            throws Exception
    {
        String tableName = "testredshiftstatisticsreader_" + randomNameSuffix();
        String schemaAndTable = TEST_SCHEMA + "." + tableName;
        try {
            executeInRedshift("CREATE TABLE " + schemaAndTable + " AS " + values);
            executeInRedshift("ANALYZE VERBOSE " + schemaAndTable);
            return statsReader.readTableStatistics(
                    SESSION,
                    new JdbcTableHandle(
                            new SchemaTableName(TEST_SCHEMA, tableName),
                            new RemoteTableName(Optional.empty(), Optional.of(TEST_SCHEMA), tableName),
                            Optional.empty()),
                    () -> columnHandles);
        }
        finally {
            executeInRedshift("DROP TABLE IF EXISTS " + schemaAndTable);
        }
    }

    private static JdbcColumnHandle createVarcharJdbcColumnHandle(String name, int length)
    {
        return new JdbcColumnHandle(
                name,
                new JdbcTypeHandle(Types.VARCHAR, Optional.of("varchar"), Optional.of(length), Optional.empty(), Optional.empty(), Optional.empty()),
                VarcharType.createVarcharType(length));
    }
}
