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
package io.trino.plugin.starrocks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.SystemSessionProperties.MARK_DISTINCT_STRATEGY;
import static io.trino.plugin.starrocks.StarRocksQueryRunner.createStarRocksQueryRunner;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStarRocksConnectorTest
        extends BaseJdbcConnectorTest
{
    protected TestingStarRocksServer starRocksServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        starRocksServer = closeAfterClass(new TestingStarRocksServer());
        return createStarRocksQueryRunner(
                starRocksServer,
                ImmutableMap.of(),
                ImmutableMap.of("starrocks.olap-default-replication-number", "1"),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN,
                    SUPPORTS_TOPN_PUSHDOWN_WITH_VARCHAR -> true;
            case SUPPORTS_ADD_COLUMN,
                    SUPPORTS_AGGREGATION_PUSHDOWN_CORRELATION,
                    SUPPORTS_AGGREGATION_PUSHDOWN_COVARIANCE,
                    SUPPORTS_AGGREGATION_PUSHDOWN_REGRESSION,
                    SUPPORTS_AGGREGATION_PUSHDOWN_STDDEV,
                    SUPPORTS_AGGREGATION_PUSHDOWN_VARIANCE,
                    SUPPORTS_ARRAY,
                    SUPPORTS_COMMENT_ON_TABLE,
                    SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT,
                    SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT,
                    SUPPORTS_CREATE_MATERIALIZED_VIEW,
                    SUPPORTS_CREATE_VIEW,
                    SUPPORTS_DELETE,
                    SUPPORTS_DROP_NOT_NULL_CONSTRAINT,
                    SUPPORTS_DROP_SCHEMA_CASCADE,
                    SUPPORTS_NATIVE_QUERY,
                    SUPPORTS_NEGATIVE_DATE,
                    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY,
                    SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY,
                    SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN,
                    SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS,
                    SUPPORTS_ROW_TYPE,
                    SUPPORTS_RENAME_COLUMN,
                    SUPPORTS_SET_COLUMN_TYPE,
                    SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    public void testCreateTableFailWithFloatOrDoubleTypeInDistributionDesc()
    {
        String tableName = "test_float_double_type_in_distribution_desc_" + randomNameSuffix();

        assertQueryFails("CREATE TABLE " + tableName + "(a bigint, b real, c double, d decimal(19, 1)) WITH (distribution_desc = ARRAY['b'])",
                "Unexpected exception: FLOAT column can not be distribution column");
        assertQueryFails("CREATE TABLE " + tableName + "(a bigint, b real, c double, d decimal(19, 1)) WITH (distribution_desc = ARRAY['a', 'b'])",
                "Unexpected exception: FLOAT column can not be distribution column");
        assertQueryFails("CREATE TABLE " + tableName + "(a bigint, b real, c double, d decimal(19, 1)) WITH (distribution_desc = ARRAY['b', 'c'])",
                "Unexpected exception: FLOAT column can not be distribution column");

        assertQueryFails("CREATE TABLE " + tableName + "(a bigint, b real, c double, d decimal(19, 1)) WITH (distribution_desc = ARRAY['c'])",
                "Unexpected exception: DOUBLE column can not be distribution column");
        assertQueryFails("CREATE TABLE " + tableName + "(a bigint, b real, c double, d decimal(19, 1)) WITH (distribution_desc = ARRAY['c', 'd'])",
                "Unexpected exception: DOUBLE column can not be distribution column");
    }

    @Test
    public void testCreateTableWithNonExistingDistributionDesc()
    {
        String tableName = "test_non_exists_column_in_distribution_desc_" + randomNameSuffix();
        assertQueryFails("CREATE TABLE " + tableName + "(a bigint) WITH (distribution_desc = ARRAY['not_existing_column'])",
                "Column 'not_existing_column' specified in property 'distribution_desc' doesn't exist in table");

        assertQueryFails("CREATE TABLE " + tableName + "(a bigint) WITH (distribution_desc = ARRAY['a', 'b'])",
                "Column 'b' specified in property 'distribution_desc' doesn't exist in table");

        assertQueryFails("CREATE TABLE " + tableName + "(a bigint) WITH (distribution_desc = ARRAY['A'])",
                "Column 'A' specified in property 'distribution_desc' doesn't exist in table");
    }

    @Test
    public void testCreateTableWithAllProperties()
    {
        String tableWithAllProperties = "test_create_with_all_properties_" + randomNameSuffix();
        @Language("SQL") String sql = """
                CREATE TABLE %s (
                   a bigint,
                   b char(123),
                   c varchar(1),
                   d date,
                   e decimal(12, 2),
                   f real,
                   g double)
                WITH (
                   distribution_desc = ARRAY['a', 'b', 'c', 'd', 'e'],
                   replication_num = 1,
                   engine = 'OLAP'\n)""".formatted(tableWithAllProperties);
        assertUpdate(sql);
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "tpch.table",
                tableDefinition("(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT \"43\"," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT \"42\"," +
                        "col_required2 BIGINT NOT NULL)", "col_required"));
    }

    @Override
    public void testShowCreateTable()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String expected = """
                CREATE TABLE %s.%s.orders (
                   orderkey bigint,
                   custkey bigint,
                   orderstatus varchar(1),
                   totalprice double,
                   orderdate date,
                   orderpriority varchar(15),
                   clerk varchar(15),
                   shippriority integer,
                   comment varchar(79)\n)
                WITH (
                   distribution_desc = ARRAY['orderkey'],
                   engine = 'OLAP',
                   replication_num = 1\n)""".formatted(catalog, schema);

        assertThat(computeScalar("SHOW CREATE TABLE orders")).isEqualTo(expected);
    }

    @Override
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        switch (columnName) {
            case "a/slash`":
            case "a`backtick`":
            case "a\\backslash`":
                return Optional.empty();
        }

        return Optional.of(columnName);
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        switch (dataMappingTestSetup.getTrinoTypeName()) {
            case "time":
            case "time(6)":
            case "timestamp":
            case "timestamp(6)":
            case "timestamp(3) with time zone":
            case "timestamp(6) with time zone":
            case "varchar":
            case "varbinary":
                return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return Optional.of(dataMappingTestSetup);
    }

    @Override
    public void testDateYearOfEraPredicate()
    {
        // Override because the connector throws an exception instead of an empty result when the value is out of supported range
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
        assertQueryFails("SELECT * FROM orders WHERE orderdate = DATE '-1996-09-14'", errorMessageForDateOutOfRange("-1996-09-14"));
    }

    @Override
    protected String tableDefinitionForTestingVarcharCastToDateInPredicate()
    {
        return "(a varchar(20))";
    }

    // Override because the execution will fail while copying data from temp table to target table even the temp table is empty
    @Override
    public void testInsertIntoNotNullColumn()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "insert_not_null", "(nullable_col INTEGER, not_null_col INTEGER NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            // The error message comes from remote databases when ConnectorMetadata.supportsMissingColumnsOnInsert is true
            assertQueryFails(format("INSERT INTO %s (nullable_col) VALUES (1)", table.getName()), errorMessageForInsertIntoNotNullColumn("not_null_col"));
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (NULL, 3)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (TRY(5/0), 4)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            assertQueryFails(format("INSERT INTO %s (not_null_col) VALUES (TRY(6/0))", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            assertQueryFails(format("INSERT INTO %s (nullable_col) SELECT nationkey FROM nation", table.getName()), errorMessageForInsertIntoNotNullColumn("not_null_col"));
            assertQueryFails(format("INSERT INTO %s (nullable_col) SELECT nationkey FROM nation WHERE regionkey < 0", table.getName()), errorMessageForInsertIntoNotNullColumn("not_null_col"));
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "commuted_not_null", "(nullable_col BIGINT, not_null_col BIGINT NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            // This is enforced by the engine and not the connector
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (NULL, 3)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
        }
    }

    // TODO: Remove the test.
    // Override because StarRocks return unexpected result the case "select a, b from (...) group by a, b" and only after ingesting the TPCH.ORDERS table.
    @Override
    public void testDistinctAggregationPushdown()
    {
        // SELECT DISTINCT
        assertThat(query("SELECT DISTINCT regionkey FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT min(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        try (TestTable emptyTable = createAggregationTestTable(getSession().getSchema().orElseThrow() + ".empty_table", ImmutableList.of())) {
            assertThat(query("SELECT DISTINCT a_bigint FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT min(DISTINCT a_bigint) FROM " + emptyTable.getName())).isFullyPushedDown();
            assertThat(query("SELECT DISTINCT t_double, min(a_bigint) FROM " + emptyTable.getName() + " GROUP BY t_double")).isFullyPushedDown();
        }

        Session withMarkDistinct = Session.builder(getSession())
                .setSystemProperty(MARK_DISTINCT_STRATEGY, "always")
                .build();
        // distinct aggregation
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertConditionallyPushedDown(
                withMarkDistinct,
                "SELECT count(DISTINCT comment) FROM nation",
                hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY),
                node(AggregationNode.class, node(TableScanNode.class)));
        // two distinct aggregations
        assertConditionallyPushedDown(
                withMarkDistinct,
                "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation",
                hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT),
                node(MarkDistinctNode.class, node(ExchangeNode.class, node(ExchangeNode.class, node(TableScanNode.class)))));
        assertConditionallyPushedDown(
                withMarkDistinct,
                "SELECT sum(DISTINCT regionkey), sum(DISTINCT nationkey) FROM nation",
                hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN),
                node(MarkDistinctNode.class, node(ExchangeNode.class, node(ExchangeNode.class, node(TableScanNode.class)))));
        // distinct aggregation and a non-distinct aggregation
        assertConditionallyPushedDown(
                withMarkDistinct,
                "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation",
                hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT),
                node(MarkDistinctNode.class, node(ExchangeNode.class, node(ExchangeNode.class, node(TableScanNode.class)))));
        assertConditionallyPushedDown(
                withMarkDistinct,
                "SELECT sum(DISTINCT regionkey), count(nationkey) FROM nation",
                hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN),
                node(MarkDistinctNode.class, node(ExchangeNode.class, node(ExchangeNode.class, node(TableScanNode.class)))));

        Session withoutMarkDistinct = Session.builder(getSession())
                .setSystemProperty(MARK_DISTINCT_STRATEGY, "none")
                .build();
        // distinct aggregation
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertConditionallyPushedDown(
                withoutMarkDistinct,
                "SELECT count(DISTINCT comment) FROM nation",
                hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY),
                node(AggregationNode.class, node(TableScanNode.class)));
        // two distinct aggregations
        assertConditionallyPushedDown(
                withoutMarkDistinct,
                "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation",
                hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT),
                node(AggregationNode.class, node(ExchangeNode.class, node(ExchangeNode.class, node(TableScanNode.class)))));
        assertConditionallyPushedDown(
                withoutMarkDistinct,
                "SELECT sum(DISTINCT regionkey), sum(DISTINCT nationkey) FROM nation",
                hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN),
                node(AggregationNode.class, node(ExchangeNode.class, node(ExchangeNode.class, node(TableScanNode.class)))));

        // distinct aggregation and a non-distinct aggregation
        assertConditionallyPushedDown(
                withoutMarkDistinct,
                "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation",
                hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN_COUNT_DISTINCT),
                node(AggregationNode.class, node(ExchangeNode.class, node(ExchangeNode.class, node(TableScanNode.class)))));
        assertConditionallyPushedDown(
                withoutMarkDistinct,
                "SELECT sum(DISTINCT regionkey), sum(nationkey) FROM nation",
                hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN),
                node(AggregationNode.class, node(ExchangeNode.class, node(ExchangeNode.class, node(TableScanNode.class)))));
    }

//    @Override
//    public void testReadMetadataWithRelationsConcurrentModifications()
//    {
//        try {
//            super.testReadMetadataWithRelationsConcurrentModifications();
//        }
//        catch (Exception expected) {
//            // The test failure is not guaranteed
//            assertThat(expected)
//                    .hasMessageMatching("(?s).*(Can not get table properties|" +
//                            "Error listing table columns).*");
//            throw new SkipException("to be fixed");
//        }
//    }

    @Override
    public void testCharTrailingSpace()
    {
        String schema = getSession().getSchema().orElseThrow();
        try (TestTable table = new TestTable(onRemoteDatabase(), schema + ".char_trailing_space", tableDefinition("(x char(10))", "x"), List.of("'test'"))) {
            String tableName = table.getName();
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test'", "VALUES 'test      '");
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test  '", "VALUES 'test      '");
            assertQuery("SELECT * FROM " + tableName + " WHERE x = char 'test        '", "VALUES 'test      '");
            assertQueryReturnsEmptyResult("SELECT * FROM " + tableName + " WHERE x = char ' test'");
        }
    }

    @Override
    public void testCreateTableAsSelectWithUnicode()
    {
        // Covered by testCreateTableAsSelect
        assertCreateTableAsSelect(
                "SELECT CAST('\u2603' AS varchar(10)) unicode",
                "SELECT 1");
    }

    @Override
    protected TestTable createAggregationTestTable(String name, List<String> rows)
    {
        return new TestTable(onRemoteDatabase(), name, tableDefinition("(short_decimal decimal(9, 3), long_decimal decimal(30, 10), t_double double, a_bigint bigint)", "a_bigint"), rows);
    }

    @Override
    protected TestTable createTableWithDoubleAndRealColumns(String name, List<String> rows)
    {
        return new TestTable(onRemoteDatabase(), name, tableDefinition("(a_bigint bigint, t_double double, u_double double, v_real float, w_real float)", "a_bigint"), rows);
    }

    @Override
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        return errorMessageForDateOutOfRange(date);
    }

    @Override
    protected String errorMessageForInsertNegativeDate(String date)
    {
        return errorMessageForDateOutOfRange(date);
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return ".*'%s' must be explicitly mentioned in column permutation".formatted(columnName);
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(256);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Incorrect database name .*");
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(1024);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Incorrect table name .*");
    }

    @Override
    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.of(1024);
    }

    @Override
    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("(Incorrect column name '.*'|Identifier name '.*' is too long)");
    }

    private String errorMessageForDateOutOfRange(String date)
    {
        return "Date must be between 0000-01-01 and 9999-12-31 in StarRocks: " + date;
    }

    private String tableDefinition(String tableDefinition, String distributionKey)
    {
        return tableDefinition + " DISTRIBUTED BY HASH(`%s`) PROPERTIES(\"replication_num\" = \"1\")".formatted(distributionKey);
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return starRocksServer::execute;
    }
}
