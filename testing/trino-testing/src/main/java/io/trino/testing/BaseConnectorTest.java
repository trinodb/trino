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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.cost.StatsAndCosts;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.security.Identity;
import io.trino.sql.planner.OptimizerConfig.JoinDistributionType;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.query.QueryAssertions.QueryAssert;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.testing.assertions.TrinoExceptionAssert;
import io.trino.testing.sql.TestTable;
import io.trino.testing.sql.TestView;
import io.trino.tpch.TpchTable;
import org.assertj.core.api.AssertProvider;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.SystemSessionProperties.IGNORE_STATS_CALCULATOR_FAILURES;
import static io.trino.connector.informationschema.InformationSchemaTable.INFORMATION_SCHEMA;
import static io.trino.server.testing.TestingSystemSessionProperties.TESTING_SESSION_TIME;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.FRESH;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.STALE;
import static io.trino.spi.connector.MaterializedViewFreshness.Freshness.UNKNOWN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.planprinter.PlanPrinter.textLogicalPlan;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.QueryAssertions.getTrinoExceptionCause;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN_WITH_COMMENT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_FIELD;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_FIELD_IN_ARRAY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ARRAY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_MATERIALIZED_VIEW_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_VIEW_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_FEDERATED_MATERIALIZED_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_FUNCTION;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_MATERIALIZED_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_MATERIALIZED_VIEW_GRACE_PERIOD;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_OR_REPLACE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DEREFERENCE_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DROP_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DROP_FIELD;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DROP_FIELD_IN_ARRAY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DROP_NOT_NULL_CONSTRAINT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DROP_SCHEMA_CASCADE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_MAP_TYPE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_MATERIALIZED_VIEW_FRESHNESS_FROM_BASE_TABLES;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_MERGE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_MULTI_STATEMENT_WRITES;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NEGATIVE_DATE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NOT_NULL_CONSTRAINT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_FIELD;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_MATERIALIZED_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_REPORTING_WRITTEN_BYTES;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_LEVEL_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_LEVEL_UPDATE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_TYPE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_SET_COLUMN_TYPE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_SET_FIELD_TYPE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_SET_FIELD_TYPE_IN_ARRAY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_SET_FIELD_TYPE_IN_MAP;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TRUNCATE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_UPDATE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.lang.Thread.currentThread;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;
import static org.assertj.core.api.InstanceOfAssertFactories.ZONED_DATE_TIME;
import static org.junit.jupiter.api.Assumptions.abort;

/**
 * Generic test for connectors.
 */
public abstract class BaseConnectorTest
        extends AbstractTestQueries
{
    private static final Logger log = Logger.get(BaseConnectorTest.class);

    protected static final List<TpchTable<?>> REQUIRED_TPCH_TABLES = ImmutableSet.<TpchTable<?>>builder()
            .addAll(AbstractTestQueries.REQUIRED_TPCH_TABLES)
            .add(CUSTOMER)
            .build().asList();

    private final ConcurrentMap<String, Function<ConnectorSession, List<String>>> mockTableListings = new ConcurrentHashMap<>();

    @BeforeAll
    public void initMockCatalog()
    {
        QueryRunner queryRunner = getQueryRunner();
        queryRunner.installPlugin(buildMockConnectorPlugin());
        queryRunner.createCatalog("mock_dynamic_listing", "mock", Map.of());
    }

    protected MockConnectorPlugin buildMockConnectorPlugin()
    {
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withListSchemaNames(session -> ImmutableList.copyOf(mockTableListings.keySet()))
                .withListTables((session, schemaName) ->
                        verifyNotNull(mockTableListings.get(schemaName), "No listing function registered for [%s]", schemaName)
                                .apply(session))
                .build();
        return new MockConnectorPlugin(connectorFactory);
    }

    /**
     * Make sure to group related behaviours together in the order and grouping they are declared in {@link TestingConnectorBehavior}.
     */
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return connectorBehavior.hasBehaviorByDefault(this::hasBehavior);
    }

    @Test
    @Override
    public void ensureTestNamingConvention()
    {
        // Enforce a naming convention to make code navigation easier.
        assertThat(getClass().getName())
                .endsWith("ConnectorTest");
    }

    /**
     * Ensure the tests are run with {@link io.trino.testing.DistributedQueryRunner} with multiple workers.
     */
    @Test
    public void ensureDistributedQueryRunner()
    {
        assertThat(getQueryRunner().getNodeCount()).as("query runner node count")
                .isGreaterThanOrEqualTo(3);
    }

    @Test
    public void testShowCreateSchema()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE SCHEMA " + schemaName))
                .isEqualTo(format("CREATE SCHEMA %s.%s", getSession().getCatalog().orElseThrow(), schemaName));
    }

    @Test
    public void testCreateSchema()
    {
        String schemaName = "test_schema_create_" + randomNameSuffix();
        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            assertQueryFails(createSchemaSql(schemaName), "This connector does not support creating schemas");
            return;
        }
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schemaName);
        assertUpdate(createSchemaSql(schemaName));

        // verify listing of new schema
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);

        // verify SHOW CREATE SCHEMA works
        assertThat((String) computeScalar("SHOW CREATE SCHEMA " + schemaName))
                .startsWith(format("CREATE SCHEMA %s.%s", getSession().getCatalog().orElseThrow(), schemaName));

        // try to create duplicate schema
        assertQueryFails(createSchemaSql(schemaName), format("line 1:1: Schema '.*\\.%s' already exists", schemaName));

        // cleanup
        assertUpdate("DROP SCHEMA " + schemaName);

        // verify DROP SCHEMA for non-existing schema
        assertQueryFails("DROP SCHEMA " + schemaName, format("line 1:1: Schema '.*\\.%s' does not exist", schemaName));
    }

    @Test
    public void testDropNonEmptySchemaWithTable()
    {
        String schemaName = "test_drop_non_empty_schema_table_" + randomNameSuffix();
        // A connector either supports CREATE SCHEMA and DROP SCHEMA or none of them.
        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            return;
        }

        try {
            assertUpdate(createSchemaSql(schemaName));
            assertUpdate("CREATE TABLE " + schemaName + ".t(x int)");
            assertQueryFails("DROP SCHEMA " + schemaName, ".*Cannot drop non-empty schema '\\Q" + schemaName + "\\E'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + ".t");
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    @Test
    public void testDropNonEmptySchemaWithView()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        // A connector either supports CREATE SCHEMA and DROP SCHEMA or none of them.
        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            return;
        }

        String schemaName = "test_drop_non_empty_schema_view_" + randomNameSuffix();

        try {
            assertUpdate(createSchemaSql(schemaName));
            assertUpdate("CREATE VIEW " + schemaName + ".v_t  AS SELECT 123 x");

            assertQueryFails("DROP SCHEMA " + schemaName, ".*Cannot drop non-empty schema '\\Q" + schemaName + "\\E'");
        }
        finally {
            assertUpdate("DROP VIEW IF EXISTS " + schemaName + ".v_t");
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    @Test
    public void testDropNonEmptySchemaWithMaterializedView()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW));

        // A connector either supports CREATE SCHEMA and DROP SCHEMA or none of them.
        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            return;
        }

        String schemaName = "test_drop_non_empty_schema_mv_" + randomNameSuffix();

        try {
            assertUpdate(createSchemaSql(schemaName));
            assertUpdate("CREATE MATERIALIZED VIEW " + schemaName + ".mv_t  AS SELECT 123 x");

            assertQueryFails("DROP SCHEMA " + schemaName, ".*Cannot drop non-empty schema '\\Q" + schemaName + "\\E'");
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + schemaName + ".mv_t");
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    @Test
    public void testColumnsInReverseOrder()
    {
        assertQuery("SELECT shippriority, clerk, totalprice FROM orders");
    }

    // Test char and varchar comparisons. Currently, unless such comparison is unwrapped in the engine, it's not pushed down into the connector,
    // but this can change with expression-based predicate pushdown.
    @Test
    public void testCharVarcharComparison()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_char_varchar",
                "(k, v) AS VALUES" +
                        "   (-1, CAST(NULL AS char(3))), " +
                        "   (3, CAST('   ' AS char(3)))," +
                        "   (6, CAST('x  ' AS char(3)))")) {
            // varchar of length shorter than column's length
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS varchar(2))",
                    // The value is included because both sides of the comparison are coerced to char(3)
                    "VALUES (3, '   ')");

            // varchar of length longer than column's length
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS varchar(4))",
                    // The value is included because both sides of the comparison are coerced to char(4)
                    "VALUES (3, '   ')");

            // value that's not all-spaces
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS varchar(2))",
                    // The value is included because both sides of the comparison are coerced to char(3)
                    "VALUES (6, 'x  ')");
        }
    }

    // Test varchar and char comparisons. Currently, unless such comparison is unwrapped in the engine, it's not pushed down into the connector,
    // but this can change with expression-based predicate pushdown.
    @Test
    public void testVarcharCharComparison()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_varchar_char",
                "(k, v) AS VALUES" +
                        "   (-1, CAST(NULL AS varchar(3))), " +
                        "   (0, CAST('' AS varchar(3)))," +
                        "   (1, CAST(' ' AS varchar(3))), " +
                        "   (2, CAST('  ' AS varchar(3))), " +
                        "   (3, CAST('   ' AS varchar(3)))," +
                        "   (4, CAST('x' AS varchar(3)))," +
                        "   (5, CAST('x ' AS varchar(3)))," +
                        "   (6, CAST('x  ' AS varchar(3)))")) {
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('  ' AS char(2))",
                    // The 3-spaces value is included because both sides of the comparison are coerced to char(3)
                    "VALUES (0, ''), (1, ' '), (2, '  '), (3, '   ')");

            // value that's not all-spaces
            assertQuery(
                    "SELECT k, v FROM " + table.getName() + " WHERE v = CAST('x ' AS char(2))",
                    // The 3-spaces value is included because both sides of the comparison are coerced to char(3)
                    "VALUES (4, 'x'), (5, 'x '), (6, 'x  ')");
        }
    }

    @Test
    public void testAggregation()
    {
        assertQuery("SELECT sum(orderkey) FROM orders");
        assertQuery("SELECT sum(totalprice) FROM orders");
        assertQuery("SELECT max(comment) FROM nation");

        assertQuery("SELECT count(*) FROM orders");
        assertQuery("SELECT count(*) FROM orders WHERE orderkey > 10");
        assertQuery("SELECT count(*) FROM (SELECT * FROM orders LIMIT 10)");
        assertQuery("SELECT count(*) FROM (SELECT * FROM orders WHERE orderkey > 10 LIMIT 10)");

        assertQuery("SELECT DISTINCT regionkey FROM nation");
        assertQuery("SELECT regionkey FROM nation GROUP BY regionkey");

        // TODO support aggregation pushdown with GROUPING SETS
        assertQuery(
                "SELECT regionkey, nationkey FROM nation GROUP BY GROUPING SETS ((regionkey), (nationkey))",
                "SELECT NULL, nationkey FROM nation " +
                        "UNION ALL SELECT DISTINCT regionkey, NULL FROM nation");
        assertQuery(
                "SELECT regionkey, nationkey, count(*) FROM nation GROUP BY GROUPING SETS ((), (regionkey), (nationkey), (regionkey, nationkey))",
                "SELECT NULL, NULL, count(*) FROM nation " +
                        "UNION ALL SELECT NULL, nationkey, 1 FROM nation " +
                        "UNION ALL SELECT regionkey, NULL, count(*) FROM nation GROUP BY regionkey " +
                        "UNION ALL SELECT regionkey, nationkey, 1 FROM nation");

        assertQuery("SELECT count(regionkey) FROM nation");
        assertQuery("SELECT count(DISTINCT regionkey) FROM nation");
        assertQuery("SELECT regionkey, count(*) FROM nation GROUP BY regionkey");

        assertQuery("SELECT min(regionkey), max(regionkey) FROM nation");
        assertQuery("SELECT min(DISTINCT regionkey), max(DISTINCT regionkey) FROM nation");
        assertQuery("SELECT regionkey, min(regionkey), min(name), max(regionkey), max(name) FROM nation GROUP BY regionkey");

        assertQuery("SELECT sum(regionkey) FROM nation");
        assertQuery("SELECT sum(DISTINCT regionkey) FROM nation");
        assertQuery("SELECT regionkey, sum(regionkey) FROM nation GROUP BY regionkey");

        assertQuery(
                "SELECT avg(nationkey) FROM nation",
                "SELECT avg(CAST(nationkey AS double)) FROM nation");
        assertQuery(
                "SELECT avg(DISTINCT nationkey) FROM nation",
                "SELECT avg(DISTINCT CAST(nationkey AS double)) FROM nation");
        assertQuery(
                "SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey",
                "SELECT regionkey, avg(CAST(nationkey AS double)) FROM nation GROUP BY regionkey");

        // pruned away aggregation (simplified regression test for https://github.com/trinodb/trino/issues/12598)
        assertQuery(
                "SELECT -13 FROM (SELECT count(*) FROM nation)",
                "VALUES -13");
        // regression test for https://github.com/trinodb/trino/issues/12598
        assertQuery(
                "SELECT count(*) FROM (SELECT count(*) FROM nation UNION ALL SELECT count(*) FROM region)",
                "VALUES 2");

        // HAVING, i.e. filter after aggregation
        assertQuery("SELECT count(*) FROM nation HAVING count(*) = 25");
        assertQuery("SELECT regionkey, count(*) FROM nation GROUP BY regionkey HAVING count(*) = 5");
        assertQuery(
                "SELECT regionkey, count(*) FROM nation GROUP BY GROUPING SETS ((), (regionkey)) HAVING count(*) IN (5, 25)",
                "(SELECT NULL, count(*) FROM nation) UNION ALL (SELECT regionkey, count(*) FROM nation GROUP BY regionkey)");
    }

    @Test
    public void testExactPredicate()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM orders WHERE orderkey = 10");

        // filtered column is selected
        assertQuery("SELECT custkey, orderkey FROM orders WHERE orderkey = 32", "VALUES (1301, 32)");

        // filtered column is not selected
        assertQuery("SELECT custkey FROM orders WHERE orderkey = 32", "VALUES (1301)");
    }

    @Test
    public void testInListPredicate()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM orders WHERE orderkey IN (10, 11, 20, 21)");

        // filtered column is selected
        assertQuery("SELECT custkey, orderkey FROM orders WHERE orderkey IN (7, 10, 32, 33)", "VALUES (392, 7), (1301, 32), (670, 33)");

        // filtered column is not selected
        assertQuery("SELECT custkey FROM orders WHERE orderkey IN (7, 10, 32, 33)", "VALUES (392), (1301), (670)");
    }

    @Test
    public void testIsNullPredicate()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM orders WHERE orderkey IS NULL");
        assertQueryReturnsEmptyResult("SELECT * FROM orders WHERE orderkey = 10 OR orderkey IS NULL");

        // filtered column is selected
        assertQuery("SELECT custkey, orderkey FROM orders WHERE orderkey = 32 OR orderkey IS NULL", "VALUES (1301, 32)");

        // filtered column is not selected
        assertQuery("SELECT custkey FROM orders WHERE orderkey = 32 OR orderkey IS NULL", "VALUES (1301)");
    }

    @Test
    public void testLikePredicate()
    {
        // filtered column is not selected
        assertQuery("SELECT orderkey FROM orders WHERE orderpriority LIKE '5-L%'");

        // filtered column is selected
        assertQuery("SELECT orderkey, orderpriority FROM orders WHERE orderpriority LIKE '5-L%'");

        // filtered column is not selected
        assertQuery("SELECT orderkey FROM orders WHERE orderpriority LIKE '5-L__'");

        // filtered column is selected
        assertQuery("SELECT orderkey, orderpriority FROM orders WHERE orderpriority LIKE '5-L__'");
    }

    @Test
    public void testMultipleRangesPredicate()
    {
        // List columns explicitly. Some connectors do not maintain column ordering.
        assertQuery("" +
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                "FROM orders " +
                "WHERE orderkey BETWEEN 10 AND 50 OR orderkey BETWEEN 100 AND 150");
    }

    @Test
    public void testRangePredicate()
    {
        // List columns explicitly. Some connectors do not maintain column ordering.
        assertQuery("" +
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                "FROM orders " +
                "WHERE orderkey BETWEEN 10 AND 50");
    }

    @Test
    public void testDateYearOfEraPredicate()
    {
        // Verify the predicate of '-1996-09-14' doesn't match '1997-09-14'. Both values return same formatted string when we use 'yyyy-MM-dd' in DateTimeFormatter
        assertQuery("SELECT orderdate FROM orders WHERE orderdate = DATE '1997-09-14'", "VALUES DATE '1997-09-14'");
        assertQueryReturnsEmptyResult("SELECT * FROM orders WHERE orderdate = DATE '-1996-09-14'");
    }

    @Test
    public void testPredicateReflectedInExplain()
    {
        // Even if the predicate is pushed down into the table scan, it should still be reflected in EXPLAIN (via ConnectorTableHandle.toString)
        assertExplain(
                "EXPLAIN SELECT name FROM nation WHERE nationkey = 42",
                "(predicate|filterPredicate|constraint).{0,10}(nationkey|NATIONKEY)");
    }

    @Test
    public void testSortItemsReflectedInExplain()
    {
        // Even if the sort items are pushed down into the table scan, it should still be reflected in EXPLAIN (via ConnectorTableHandle.toString)
        @Language("RegExp") String expectedPattern = hasBehavior(SUPPORTS_TOPN_PUSHDOWN)
                ? "sortOrder=\\[(?i:nationkey):.* DESC NULLS LAST] limit=5"
                : "\\[count = 5, orderBy = \\[(?i:nationkey) DESC NULLS LAST]]";

        assertExplain(
                "EXPLAIN SELECT name FROM nation ORDER BY nationkey DESC NULLS LAST LIMIT 5",
                expectedPattern);
    }

    // CAST(a_varchar AS date) = DATE '...' has a special handling in the DomainTranslator and is worth testing this across connectors
    @Test
    public void testVarcharCastToDateInPredicate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "varchar_as_date_pred",
                "(a varchar)",
                List.of(
                        "'999-09-09'",
                        "'1005-09-09'",
                        "'2005-06-06'", "'2005-06-6'", "'2005-6-06'", "'2005-6-6'", "' 2005-06-06'", "'2005-06-06 '", "' +2005-06-06'", "'02005-06-06'",
                        "'2005-09-06'", "'2005-09-6'", "'2005-9-06'", "'2005-9-6'", "' 2005-09-06'", "'2005-09-06 '", "' +2005-09-06'", "'02005-09-06'",
                        "'2005-09-09'", "'2005-09-9'", "'2005-9-09'", "'2005-9-9'", "' 2005-09-09'", "'2005-09-09 '", "' +2005-09-09'", "'02005-09-09'",
                        "'2005-09-10'", "'2005-9-10'", "' 2005-09-10'", "'2005-09-10 '", "' +2005-09-10'", "'02005-09-10'",
                        "'2005-09-20'", "'2005-9-20'", "' 2005-09-20'", "'2005-09-20 '", "' +2005-09-20'", "'02005-09-20'",
                        "'9999-09-09'",
                        "'99999-09-09'"))) {
            for (String date : List.of("2005-09-06", "2005-09-09", "2005-09-10")) {
                for (String operator : List.of("=", "<=", "<", ">", ">=", "!=", "IS DISTINCT FROM", "IS NOT DISTINCT FROM")) {
                    assertThat(query("SELECT a FROM %s WHERE CAST(a AS date) %s DATE '%s'".formatted(table.getName(), operator, date)))
                            .hasCorrectResultsRegardlessOfPushdown();
                }
            }
        }

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "varchar_as_date_pred",
                "(a varchar)",
                List.of("'2005-06-bad-date'", "'2005-09-10'"))) {
            assertThat(query("SELECT a FROM %s WHERE CAST(a AS date) < DATE '2005-09-10'".formatted(table.getName())))
                    .failure().hasMessage("Value cannot be cast to date: 2005-06-bad-date");
            verifyResultOrFailure(
                    query("SELECT a FROM %s WHERE CAST(a AS date) = DATE '2005-09-10'".formatted(table.getName())),
                    queryAssert -> queryAssert
                            .skippingTypesCheck()
                            .matches("VALUES '2005-09-10'"),
                    failureAssert -> failureAssert
                            .hasMessage("Value cannot be cast to date: 2005-06-bad-date"));
            // This failure isn't guaranteed: a row may be filtered out on the connector side with a derived predicate on a varchar column.
            verifyResultOrFailure(
                    query("SELECT a FROM %s WHERE CAST(a AS date) != DATE '2005-9-1'".formatted(table.getName())),
                    queryAssert -> queryAssert
                            .skippingTypesCheck()
                            .matches("VALUES '2005-09-10'"),
                    failureAssert -> failureAssert
                            .hasMessage("Value cannot be cast to date: 2005-06-bad-date"));
            // This failure isn't guaranteed: a row may be filtered out on the connector side with a derived predicate on a varchar column.
            verifyResultOrFailure(
                    query("SELECT a FROM %s WHERE CAST(a AS date) > DATE '2022-08-10'".formatted(table.getName())),
                    queryAssert -> queryAssert
                            .skippingTypesCheck()
                            .returnsEmptyResult(),
                    failureAssert -> failureAssert
                            .hasMessage("Value cannot be cast to date: 2005-06-bad-date"));
        }
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "varchar_as_date_pred",
                "(a varchar)",
                List.of("'2005-09-10'"))) {
            // 2005-09-01, when written as 2005-09-1, is a prefix of an existing data point: 2005-09-10
            assertThat(query("SELECT a FROM %s WHERE CAST(a AS date) != DATE '2005-09-01'".formatted(table.getName())))
                    .skippingTypesCheck()
                    .matches("VALUES '2005-09-10'");
        }
    }

    private static void verifyResultOrFailure(AssertProvider<QueryAssert> queryAssertProvider, Consumer<QueryAssert> verifyResults, Consumer<TrinoExceptionAssert> verifyFailure)
    {
        requireNonNull(verifyResults, "verifyResults is null");
        requireNonNull(verifyFailure, "verifyFailure is null");

        QueryAssert queryAssert = assertThat(queryAssertProvider);
        try {
            var ignored = queryAssert.result();
        }
        catch (Throwable t) {
            verifyFailure.accept(queryAssert.failure());
            return;
        }
        verifyResults.accept(queryAssert);
    }

    @Test
    public void testConcurrentScans()
    {
        String unionMultipleTimes = join(" UNION ALL ", nCopies(25, "SELECT * FROM orders"));
        assertQuery("SELECT sum(if(rand() >= 0, orderkey)) FROM (" + unionMultipleTimes + ")", "VALUES 11246812500");
    }

    @Test
    public void testSelectAll()
    {
        assertQuery("SELECT * FROM orders");
    }

    @Test
    public void testSelectInTransaction()
    {
        inTransaction(session -> {
            assertQuery(session, "SELECT nationkey, name, regionkey FROM nation");
            assertQuery(session, "SELECT regionkey, name FROM region");
            assertQuery(session, "SELECT nationkey, name, regionkey FROM nation");
        });
    }

    @Test
    public void testSelectVersionOfNonExistentTable()
    {
        String tableName = "foo_" + randomNameSuffix();
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        assertThat(query("SELECT * FROM " + tableName + " FOR TIMESTAMP AS OF TIMESTAMP '2021-03-01 00:00:01'"))
                .failure().hasMessageMatching("line 1:15: Table '%s.%s.%s' does not exist|This connector does not support versioned tables".formatted(catalog, schema, tableName));
        assertThat(query("SELECT * FROM " + tableName + " FOR VERSION AS OF 'version1'"))
                .failure().hasMessageMatching("line 1:15: Table '%s.%s.%s' does not exist|This connector does not support versioned tables".formatted(catalog, schema, tableName));
    }

    /**
     * A connector can support FOR TIMESTAMP, FOR VERSION, both or none. With FOR TIMESTAMP/VERSION is can support some types but not the others.
     * Because of version support being multidimensional, {@link TestingConnectorBehavior} is not defined. The test verifies that query doesn't fail in
     * some weird way, serving as a smoke test for versioning. The purpose of the test is to validate the connector does proper validation.
     */
    @Test
    public void testTrySelectTableVersion()
    {
        testTrySelectTableVersion("SELECT * FROM nation FOR TIMESTAMP AS OF DATE '2005-09-10'");
        testTrySelectTableVersion("SELECT * FROM nation FOR TIMESTAMP AS OF TIMESTAMP '2005-09-10 13:00:00'");
        testTrySelectTableVersion("SELECT * FROM nation FOR TIMESTAMP AS OF TIMESTAMP '2005-09-10 13:00:00 Europe/Warsaw'");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF TINYINT '123'");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF SMALLINT '123'");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF 123");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF BIGINT '123'");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF REAL '123.123'");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF DOUBLE '123.123'");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF DECIMAL '123.123'");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF CHAR 'abc'");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF '123'");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF CAST('abc' AS varchar(5))");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF CAST('abc' AS varchar)");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF DATE '2005-09-10'");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF TIME '13:00:00'");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF TIMESTAMP '2005-09-10 13:00:00'");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF TIMESTAMP '2005-09-10 13:00:00 Europe/Warsaw'");
        testTrySelectTableVersion("SELECT * FROM nation FOR VERSION AS OF JSON '{}'");
    }

    private void testTrySelectTableVersion(@Language("SQL") String query)
    {
        try {
            computeActual(query);
        }
        catch (Exception somewhatExpected) {
            verifyVersionedQueryFailurePermissible(getTrinoExceptionCause(somewhatExpected));
        }
    }

    protected void verifyVersionedQueryFailurePermissible(Exception e)
    {
        assertThat(e).hasMessageContaining("This connector does not support versioned tables");
    }

    /**
     * Test interactions between optimizer (including CBO), scheduling and connector metadata APIs.
     */
    @Test
    public void testJoinWithEmptySides()
    {
        for (JoinDistributionType joinDistributionType : JoinDistributionType.values()) {
            Session session = noJoinReordering(joinDistributionType);
            // empty build side
            assertQuery(session, "SELECT count(*) FROM nation JOIN region ON nation.regionkey = region.regionkey AND region.name = ''", "VALUES 0");
            assertQuery(session, "SELECT count(*) FROM nation JOIN region ON nation.regionkey = region.regionkey AND region.regionkey < 0", "VALUES 0");
            // empty probe side
            assertQuery(session, "SELECT count(*) FROM region JOIN nation ON nation.regionkey = region.regionkey AND region.name = ''", "VALUES 0");
            assertQuery(session, "SELECT count(*) FROM nation JOIN region ON nation.regionkey = region.regionkey AND region.regionkey < 0", "VALUES 0");
        }
    }

    /**
     * Test interactions between optimizer (including CBO) and connector metadata APIs.
     */
    @Test
    public void testJoin()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(IGNORE_STATS_CALCULATOR_FAILURES, "false")
                .build();

        // 2 inner joins, eligible for join reodering
        assertQuery(
                session,
                "SELECT c.name, n.name, r.name " +
                        "FROM nation n " +
                        "JOIN customer c ON c.nationkey = n.nationkey " +
                        "JOIN region r ON n.regionkey = r.regionkey");

        // 2 inner joins, eligible for join reodering, where one table has a filter
        assertQuery(
                session,
                "SELECT c.name, n.name, r.name " +
                        "FROM nation n " +
                        "JOIN customer c ON c.nationkey = n.nationkey " +
                        "JOIN region r ON n.regionkey = r.regionkey " +
                        "WHERE n.name = 'ARGENTINA'");

        // 2 inner joins, eligible for join reodering, on top of aggregation
        assertQuery(
                session,
                "SELECT c.name, n.name, n.count, r.name " +
                        "FROM (SELECT name, regionkey, nationkey, count(*) count FROM nation GROUP BY name, regionkey, nationkey) n " +
                        "JOIN customer c ON c.nationkey = n.nationkey " +
                        "JOIN region r ON n.regionkey = r.regionkey");
    }

    @Test
    public void testDescribeTable()
    {
        // TODO: this is redundant with testShowColumns()
        assertThat(query("DESCRIBE orders")).result().matches(getDescribeOrdersResult());
    }

    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("orderkey", "bigint", "", "")
                .row("custkey", "bigint", "", "")
                .row("orderstatus", "varchar(1)", "", "")
                .row("totalprice", "double", "", "")
                .row("orderdate", "date", "", "")
                .row("orderpriority", "varchar(15)", "", "")
                .row("clerk", "varchar(15)", "", "")
                .row("shippriority", "integer", "", "")
                .row("comment", "varchar(79)", "", "")
                .build();
    }

    @Test
    public void testShowInformationSchemaTables()
    {
        assertThat(query("SHOW TABLES FROM information_schema"))
                .skippingTypesCheck()
                .containsAll("VALUES 'applicable_roles', 'columns', 'enabled_roles', 'roles', 'schemata', 'table_privileges', 'tables', 'views'");
    }

    @Test
    public void testView()
    {
        if (!hasBehavior(SUPPORTS_CREATE_VIEW)) {
            assertQueryFails("CREATE VIEW nation_v AS SELECT * FROM nation", "This connector does not support creating views");
            return;
        }

        @Language("SQL") String query = "SELECT orderkey, orderstatus, (totalprice / 2) half FROM orders";

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        String testView = "test_view_" + randomNameSuffix();
        String testViewWithComment = "test_view_with_comment_" + randomNameSuffix();
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()) // prime the cache, if any
                .doesNotContain(testView);
        assertUpdate("CREATE VIEW " + testView + " AS SELECT 123 x");
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .contains(testView);
        assertUpdate("CREATE OR REPLACE VIEW " + testView + " AS " + query);

        assertUpdate("CREATE VIEW " + testViewWithComment + " COMMENT 'orders' AS SELECT 123 x");
        assertUpdate("CREATE OR REPLACE VIEW " + testViewWithComment + " COMMENT 'orders' AS " + query);

        // verify comment
        assertThat((String) computeScalar("SHOW CREATE VIEW " + testViewWithComment)).contains("COMMENT 'orders'");
        assertThat(query(
                "SELECT table_name, comment FROM system.metadata.table_comments " +
                        "WHERE catalog_name = '" + catalogName + "' AND " +
                        "schema_name = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + testView + "', null), ('" + testViewWithComment + "', 'orders')");

        // reading
        assertQuery("SELECT * FROM " + testView, query);
        assertQuery("SELECT * FROM " + testViewWithComment, query);

        assertQuery(
                "SELECT * FROM " + testView + " a JOIN " + testView + " b on a.orderkey = b.orderkey",
                format("SELECT * FROM (%s) a JOIN (%s) b ON a.orderkey = b.orderkey", query, query));

        assertQuery("WITH orders AS (SELECT * FROM orders LIMIT 0) SELECT * FROM " + testView, query);

        String name = format("%s.%s." + testView, catalogName, schemaName);
        assertQuery("SELECT * FROM " + name, query);

        assertUpdate("DROP VIEW " + testViewWithComment);

        // information_schema.views without table_name filter
        assertThat(query(
                "SELECT table_name, regexp_replace(view_definition, '\\s', '') FROM information_schema.views " +
                        "WHERE table_schema = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + testView + "', '" + query.replaceAll("\\s", "") + "')");
        // information_schema.views with table_name filter
        assertQuery(
                "SELECT table_name, regexp_replace(view_definition, '\\s', '') FROM information_schema.views " +
                        "WHERE table_schema = '" + schemaName + "' and table_name = '" + testView + "'",
                "VALUES ('" + testView + "', '" + query.replaceAll("\\s", "") + "')");

        // table listing
        assertThat(query("SHOW TABLES"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + testView + "'");
        // information_schema.tables without table_name filter
        assertThat(query(
                "SELECT table_name, table_type FROM information_schema.tables " +
                        "WHERE table_schema = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + testView + "', 'VIEW')");
        // information_schema.tables with table_name filter
        assertQuery(
                "SELECT table_name, table_type FROM information_schema.tables " +
                        "WHERE table_schema = '" + schemaName + "' and table_name = '" + testView + "'",
                "VALUES ('" + testView + "', 'VIEW')");

        // system.jdbc.tables without filter
        assertThat(query("SELECT table_schem, table_name, table_type FROM system.jdbc.tables"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + schemaName + "', '" + testView + "', 'VIEW')");

        // system.jdbc.tables with table prefix filter
        assertQuery(
                "SELECT table_schem, table_name, table_type " +
                        "FROM system.jdbc.tables " +
                        "WHERE table_cat = '" + catalogName + "' AND " +
                        "table_schem = '" + schemaName + "' AND " +
                        "table_name = '" + testView + "'",
                "VALUES ('" + schemaName + "', '" + testView + "', 'VIEW')");

        // column listing
        assertThat(query("SHOW COLUMNS FROM " + testView))
                .result()
                .projected("Column") // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'orderkey', 'orderstatus', 'half'");

        assertThat(query("DESCRIBE " + testView))
                .result()
                .projected("Column") // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'orderkey', 'orderstatus', 'half'");

        // information_schema.columns without table_name filter
        assertThat(query(
                "SELECT table_name, column_name " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES '" + testView + "') " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        // information_schema.columns with table_name filter
        assertThat(query(
                "SELECT table_name, column_name " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = '" + schemaName + "' and table_name = '" + testView + "'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES '" + testView + "') " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        // view-specific listings
        assertThat(query("SELECT table_name FROM information_schema.views WHERE table_schema = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + testView + "'");

        // system.jdbc.columns without filter
        assertThat(query("SELECT table_schem, table_name, column_name FROM system.jdbc.columns"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + schemaName + "', '" + testView + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        // system.jdbc.columns with schema filter
        assertThat(query(
                "SELECT table_schem, table_name, column_name " +
                        "FROM system.jdbc.columns " +
                        "WHERE table_schem LIKE '%" + schemaName + "%'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + schemaName + "', '" + testView + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        // system.jdbc.columns with table filter
        assertThat(query(
                "SELECT table_schem, table_name, column_name " +
                        "FROM system.jdbc.columns " +
                        "WHERE table_name LIKE '%" + testView + "%'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + schemaName + "', '" + testView + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        assertUpdate("DROP VIEW " + testView);
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .doesNotContain(testView);
    }

    @Test
    public void testCreateViewSchemaNotFound()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        String schemaName = "test_schema_" + randomNameSuffix();
        String viewName = "test_view_create_no_schema_" + randomNameSuffix();
        try {
            assertQueryFails(
                    format("CREATE VIEW %s.%s AS SELECT 1 AS c1", schemaName, viewName),
                    format("Schema %s not found", schemaName));
            assertQueryFails(
                    format("CREATE OR REPLACE VIEW %s.%s AS SELECT 1 AS c1", schemaName, viewName),
                    format("Schema %s not found", schemaName));
        }
        finally {
            assertUpdate(format("DROP VIEW IF EXISTS %s.%s", schemaName, viewName));
        }
    }

    @Test
    public void testViewCaseSensitivity()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        String upperCaseView = "test_view_uppercase_" + randomNameSuffix();
        String mixedCaseView = "test_view_mixedcase_" + randomNameSuffix();

        computeActual("CREATE VIEW " + upperCaseView + " AS SELECT X FROM (SELECT 123 X)");
        computeActual("CREATE VIEW " + mixedCaseView + " AS SELECT XyZ FROM (SELECT 456 XyZ)");
        assertQuery("SELECT * FROM " + upperCaseView, "SELECT X FROM (SELECT 123 X)");
        assertQuery("SELECT * FROM " + mixedCaseView, "SELECT XyZ FROM (SELECT 456 XyZ)");

        assertUpdate("DROP VIEW " + upperCaseView);
        assertUpdate("DROP VIEW " + mixedCaseView);
    }

    @Test
    public void testMaterializedView()
    {
        if (!hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW)) {
            assertQueryFails("CREATE MATERIALIZED VIEW nation_mv AS SELECT * FROM nation", "This connector does not support creating materialized views");
            return;
        }

        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String otherSchema = "other_schema" + randomNameSuffix();
        assertUpdate(createSchemaSql(otherSchema));

        QualifiedObjectName view = new QualifiedObjectName(catalog, schema, "test_materialized_view_" + randomNameSuffix());
        QualifiedObjectName otherView = new QualifiedObjectName(catalog, otherSchema, "test_materialized_view_" + randomNameSuffix());
        QualifiedObjectName viewWithComment = new QualifiedObjectName(catalog, schema, "test_materialized_view_with_comment_" + randomNameSuffix());

        createTestingMaterializedView(view, Optional.empty());
        createTestingMaterializedView(otherView, Optional.of("sarcastic comment"));
        createTestingMaterializedView(viewWithComment, Optional.of("mv_comment"));

        // verify comment
        assertThat((String) computeScalar("SHOW CREATE MATERIALIZED VIEW " + viewWithComment)).contains("COMMENT 'mv_comment'");
        assertThat(query(
                "SELECT table_name, comment FROM system.metadata.table_comments " +
                        "WHERE catalog_name = '" + view.catalogName() + "' AND " +
                        "schema_name = '" + view.schemaName() + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + view.objectName() + "', null), ('" + viewWithComment.objectName() + "', 'mv_comment')");

        // reading
        assertThat(query("SELECT * FROM " + view))
                .skippingTypesCheck()
                .matches("SELECT * FROM nation");
        assertThat(query("SELECT * FROM " + viewWithComment))
                .skippingTypesCheck()
                .matches("SELECT * FROM nation");

        // table listing
        assertThat(query("SHOW TABLES"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + view.objectName() + "'");
        // information_schema.tables without table_name filter so that ConnectorMetadata.listViews is exercised
        assertThat(query(
                "SELECT table_name, table_type FROM information_schema.tables " +
                        "WHERE table_schema = '" + view.schemaName() + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + view.objectName() + "', 'BASE TABLE')");
        // information_schema.tables with table_name filter
        assertQuery(
                "SELECT table_name, table_type FROM information_schema.tables " +
                        "WHERE table_schema = '" + view.schemaName() + "' and table_name = '" + view.objectName() + "'",
                "VALUES ('" + view.objectName() + "', 'BASE TABLE')");

        // system.jdbc.tables without filter
        assertThat(query("SELECT table_schem, table_name, table_type FROM system.jdbc.tables"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + view.schemaName() + "', '" + view.objectName() + "', 'TABLE')");

        // system.jdbc.tables with table prefix filter
        assertQuery(
                "SELECT table_schem, table_name, table_type " +
                        "FROM system.jdbc.tables " +
                        "WHERE table_cat = '" + view.catalogName() + "' AND " +
                        "table_schem = '" + view.schemaName() + "' AND " +
                        "table_name = '" + view.objectName() + "'",
                "VALUES ('" + view.schemaName() + "', '" + view.objectName() + "', 'TABLE')");

        // column listing
        assertThat(query("SHOW COLUMNS FROM " + view.objectName()))
                .result()
                .projected("Column") // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'nationkey', 'name', 'regionkey', 'comment'");

        assertThat(query("DESCRIBE " + view.objectName()))
                .result()
                .projected("Column") // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'nationkey', 'name', 'regionkey', 'comment'");

        // information_schema.columns without table_name filter
        assertThat(query(
                "SELECT table_name, column_name " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = '" + view.schemaName() + "'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES '" + view.objectName() + "') " +
                                "CROSS JOIN UNNEST(ARRAY['nationkey', 'name', 'regionkey', 'comment'])");

        // information_schema.columns with table_name filter
        assertThat(query(
                "SELECT table_name, column_name " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = '" + view.schemaName() + "' and table_name = '" + view.objectName() + "'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES '" + view.objectName() + "') " +
                                "CROSS JOIN UNNEST(ARRAY['nationkey', 'name', 'regionkey', 'comment'])");

        // view-specific listings
        assertThat(computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = '" + view.schemaName() + "'").getOnlyColumnAsSet())
                .doesNotContain(view.objectName());
        assertThat(query("SELECT table_name FROM information_schema.views WHERE table_schema = '" + view.schemaName() + "' AND table_name = '" + view.objectName() + "'"))
                .returnsEmptyResult();

        // materialized view-specific listings
        assertThat(query("SELECT name FROM system.metadata.materialized_views WHERE catalog_name = '" + catalog + "' AND schema_name = '" + view.schemaName() + "'"))
                .containsAll("VALUES VARCHAR '" + view.objectName() + "'");

        // system.jdbc.columns without filter
        assertThat(query("SELECT table_schem, table_name, column_name FROM system.jdbc.columns"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + view.schemaName() + "', '" + view.objectName() + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['nationkey', 'name', 'regionkey', 'comment'])");

        // system.jdbc.columns with schema filter
        assertThat(query(
                "SELECT table_schem, table_name, column_name " +
                        "FROM system.jdbc.columns " +
                        "WHERE table_schem LIKE '%" + view.schemaName() + "%'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + view.schemaName() + "', '" + view.objectName() + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['nationkey', 'name', 'regionkey', 'comment'])");

        // system.jdbc.columns with table filter
        assertThat(query(
                "SELECT table_schem, table_name, column_name " +
                        "FROM system.jdbc.columns " +
                        "WHERE table_name LIKE '%" + view.objectName() + "%'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + view.schemaName() + "', '" + view.objectName() + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['nationkey', 'name', 'regionkey', 'comment'])");

        // details
        assertThat(((String) computeScalar("SHOW CREATE MATERIALIZED VIEW " + view.objectName())))
                .matches("(?s)" +
                        "CREATE MATERIALIZED VIEW \\Q" + view + "\\E" +
                        ".* AS\n" +
                        "SELECT \\*\n" +
                        "FROM\n" +
                        "  nation");

        // we only want to test filtering materialized views in different schemas,
        // `viewWithComment` is in the same schema as `view` so it is not needed
        assertUpdate("DROP MATERIALIZED VIEW " + viewWithComment);

        // test filtering materialized views in system metadata table
        assertThat(query(listMaterializedViewsSql("catalog_name = '" + view.catalogName() + "'")))
                .skippingTypesCheck()
                .containsAll(getTestingMaterializedViewsResultRows(view, otherView));

        assertThat(query(
                listMaterializedViewsSql(
                        "catalog_name = '" + otherView.catalogName() + "'",
                        "schema_name = '" + otherView.schemaName() + "'")))
                .skippingTypesCheck()
                .containsAll(getTestingMaterializedViewsResultRow(otherView, "sarcastic comment"));

        assertThat(query(
                listMaterializedViewsSql(
                        "catalog_name = '" + view.catalogName() + "'",
                        "schema_name = '" + view.schemaName() + "'",
                        "name = '" + view.objectName() + "'")))
                .skippingTypesCheck()
                .containsAll(getTestingMaterializedViewsResultRow(view, ""));

        assertThat(query(
                listMaterializedViewsSql("schema_name LIKE '%" + view.schemaName() + "%'")))
                .skippingTypesCheck()
                .containsAll(getTestingMaterializedViewsResultRow(view, ""));

        assertThat(query(
                listMaterializedViewsSql("name LIKE '%" + view.objectName() + "%'")))
                .skippingTypesCheck()
                .containsAll(getTestingMaterializedViewsResultRow(view, ""));

        // verify write in transaction
        if (!hasBehavior(SUPPORTS_MULTI_STATEMENT_WRITES)) {
            assertThatThrownBy(() -> inTransaction(session -> computeActual(session, "REFRESH MATERIALIZED VIEW " + view)))
                    .hasMessageMatching("Catalog only supports writes using autocommit: \\w+");
        }

        assertUpdate("DROP MATERIALIZED VIEW " + view);
        assertUpdate("DROP MATERIALIZED VIEW " + otherView);

        assertQueryReturnsEmptyResult(listMaterializedViewsSql("name = '" + view.objectName() + "'"));
        assertQueryReturnsEmptyResult(listMaterializedViewsSql("name = '" + otherView.objectName() + "'"));
        assertQueryReturnsEmptyResult(listMaterializedViewsSql("name = '" + viewWithComment.objectName() + "'"));

        assertUpdate("DROP SCHEMA " + otherSchema);
    }

    @Test
    public void testMaterializedViewAllTypes()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW));

        String viewName = "test_mv_all_types_" + randomNameSuffix();

        String values =
                """
                SELECT
                    true a_boolean,
                    TINYINT '67' a_tinyint,
                    SMALLINT '35' a_smallint,
                    INTEGER '-1546831166' an_integer,
                    1544323431676534245 a_bigint,
                    REAL '12345.67' a_real,
                    DOUBLE '12345.678901234' a_double,
                    CAST('1234567.8901' AS decimal(11, 4)) a_short_decimal,
                    CAST('1234567890123456789.0123456' AS decimal(26, 7)) a_long_decimal,
                    CHAR 'few chars  ' a_char,
                    CAST('some string' AS varchar(33)) a_bounded_varchar,
                    CAST('some longer string' AS varchar) an_unbounded_varchar,
                    X'65683F' a_varbinary,
                    DATE '2005-09-10' a_date,
                    TIME '13:00:00' a_time_seconds,
                    TIME '13:00:00.123' a_time_millis,
                    TIME '13:00:00.123456' a_time_micros,
                    TIME '13:00:00.123456789' a_time_nanos,
                    TIME '13:00:00 +02:00' a_time_tz__seconds,
                    TIME '13:00:00.123 +02:00' a_time_tz__millis,
                    TIME '13:00:00.123456 +02:00' a_time_tz__micros,
                    TIME '13:00:00.123456789 +02:00' a_time_tz__nanos,
                    TIMESTAMP '2005-09-10 13:00:00' a_timestamp_seconds,
                    TIMESTAMP '2005-09-10 13:00:00.123' a_timestamp_millis,
                    TIMESTAMP '2005-09-10 13:00:00.123456' a_timestamp_micros,
                    TIMESTAMP '2005-09-10 13:00:00.123456789' a_timestamp_nanos,
                    TIMESTAMP '2005-09-10 13:00:00 Europe/Warsaw' a_timestamp_tz_seconds,
                    TIMESTAMP '2005-09-10 13:00:00.123 Europe/Warsaw' a_timestamp_tz_millis,
                    TIMESTAMP '2005-09-10 13:00:00.123456 Europe/Warsaw' a_timestamp_tz_micros,
                    TIMESTAMP '2005-09-10 13:00:00.123456789 Europe/Warsaw' a_timestamp_tz_nanos,
                    UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59' a_uuid,
                    ARRAY[TIMESTAMP '2005-09-10 13:00:00.123456789'] an_array_of_timestamp_nanos,
                    map(ARRAY['key'], ARRAY[TIMESTAMP '2005-09-10 13:00:00.123456789']) a_map_with_timestamp_nanos,
                    CAST(ROW(TIMESTAMP '2005-09-10 13:00:00.123456789') AS ROW(key timestamp(9))) a_row_with_timestamp_nanos,
                """ +
                // TODO JSON (requires json_format & json_parse instead of CASTs for the conversion)
                // TODO interval, IPAddress, Geo types?
                "  'a dummy' a_dummy";

        assertUpdate("CREATE MATERIALIZED VIEW %s AS %s".formatted(viewName, values));
        assertThat(query("TABLE " + viewName))
                .matches(values);
        assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 1);
        assertThat(query("TABLE " + viewName))
                .matches(values);

        assertUpdate("DROP MATERIALIZED VIEW " + viewName);
    }

    @Test
    public void testMaterializedViewGracePeriod()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW));

        String catalog = getSession().getCatalog().orElseThrow();
        String viewName = "test_mv_grace_period_" + randomNameSuffix();

        if (!hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW_GRACE_PERIOD)) {
            assertQueryFails(
                    "CREATE MATERIALIZED VIEW " + viewName + " GRACE PERIOD INTERVAL '1' HOUR AS SELECT * FROM nation",
                    "line 1:1: Catalog '%s' does support not GRACE PERIOD".formatted(catalog));
            return;
        }

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_base_table",
                "AS TABLE region")) {
            Session defaultSession = getSession();
            Session futureSession = Session.builder(defaultSession)
                    // This gets ignored: .setStart(...)
                    .setSystemProperty(TESTING_SESSION_TIME, Instant.now().plus(1, ChronoUnit.DAYS).toString())
                    .build();

            PlanMatchPattern readFromBaseTables = anyTree(
                    node(AggregationNode.class, // final
                            anyTree(// exchanges
                                    node(AggregationNode.class, // partial
                                            node(ProjectNode.class, // format()
                                                    tableScan(table.getName()))))));
            PlanMatchPattern readFromStorageTable = node(OutputNode.class, node(TableScanNode.class));

            assertUpdate("CREATE MATERIALIZED VIEW " + viewName + " " +
                    "GRACE PERIOD INTERVAL '1' HOUR " +
                    "AS SELECT DISTINCT regionkey, format('%s', name) name FROM " + table.getName());

            String initialResults = "SELECT DISTINCT regionkey, CAST(name AS varchar) FROM region";

            // The MV is initially not fresh
            assertThat(getMaterializedViewFreshness(viewName)).isEqualTo(STALE);
            assertThat(getMaterializedViewLastFreshTime(viewName)).isEmpty();
            assertThat(query(defaultSession, "TABLE " + viewName)).hasPlan(readFromBaseTables).matches(initialResults);
            assertThat(query(futureSession, "TABLE " + viewName)).hasPlan(readFromBaseTables).matches(initialResults);

            ZonedDateTime beforeRefresh = ZonedDateTime.now();
            assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 5);
            ZonedDateTime afterRefresh = ZonedDateTime.now();

            // Right after the REFRESH, the view is FRESH (note: it could also be UNKNOWN)
            boolean supportsFresh = hasBehavior(SUPPORTS_MATERIALIZED_VIEW_FRESHNESS_FROM_BASE_TABLES);
            if (supportsFresh) {
                assertThat(getMaterializedViewFreshness(viewName)).isEqualTo(FRESH);
                assertThat(getMaterializedViewLastFreshTime(viewName))
                        .isEmpty(); // last_fresh_time should not be reported for FRESH views to avoid ambiguity when it "races with currentTimeMillis"
            }
            else {
                assertThat(getMaterializedViewFreshness(viewName)).isEqualTo(UNKNOWN);
                assertThat(getMaterializedViewLastFreshTime(viewName))
                        .get(ZONED_DATE_TIME).isBetween(beforeRefresh, afterRefresh);
            }
            assertThat(query(defaultSession, "TABLE " + viewName)).hasPlan(readFromStorageTable).matches(initialResults);
            assertThat(query(futureSession, "TABLE " + viewName))
                    .hasPlan(supportsFresh ? readFromStorageTable : readFromBaseTables)
                    .matches(initialResults);

            // Change underlying state
            ZonedDateTime beforeModification = ZonedDateTime.now();
            assertUpdate("INSERT INTO " + table.getName() + " (regionkey, name) VALUES (42, 'foo new region')", 1);
            ZonedDateTime afterModification = ZonedDateTime.now();
            String updatedResults = initialResults + " UNION ALL VALUES (42, 'foo new region')";

            // The materialization is stale now
            assertThat(getMaterializedViewFreshness(viewName)).isEqualTo(supportsFresh ? STALE : UNKNOWN);
            assertThat(getMaterializedViewLastFreshTime(viewName))
                    .get(ZONED_DATE_TIME).isBetween(
                            supportsFresh ? beforeModification : beforeRefresh,
                            supportsFresh ? afterModification : afterRefresh);
            assertThat(query(defaultSession, "TABLE " + viewName)).hasPlan(readFromStorageTable).matches(initialResults);
            assertThat(query(futureSession, "TABLE " + viewName)).hasPlan(readFromBaseTables).matches(updatedResults);

            assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 6);

            // Right after the REFRESH, the view is FRESH (or UNKNOWN)
            if (supportsFresh) {
                assertThat(getMaterializedViewFreshness(viewName)).isEqualTo(FRESH);
                assertThat(getMaterializedViewLastFreshTime(viewName))
                        .isEmpty(); // last_fresh_time should not be reported for FRESH views to avoid ambiguity when it "races with currentTimeMillis"
            }
            else {
                assertThat(getMaterializedViewFreshness(viewName)).isEqualTo(UNKNOWN);
            }

            assertThat(query(defaultSession, "TABLE " + viewName)).hasPlan(readFromStorageTable).matches(updatedResults);
            assertThat(query(futureSession, "TABLE " + viewName))
                    .hasPlan(supportsFresh ? readFromStorageTable : readFromBaseTables)
                    .matches(updatedResults);

            assertUpdate("DROP MATERIALIZED VIEW " + viewName);
        }
    }

    @Test
    public void testFederatedMaterializedView()
    {
        String viewName = "test_federated_mv_" + randomNameSuffix();
        String mockSchemaForListing = "mock_schema_for_listing_" + randomNameSuffix();

        String query = "" +
                "SELECT table_name, count(*) AS c FROM mock_dynamic_listing.information_schema.tables " +
                "WHERE table_schema = '" + mockSchemaForListing + "' " +
                "GROUP BY table_name"; // GROUP BY so that it is easy to distinguish between inlined view and reading materialization
        String create = "CREATE MATERIALIZED VIEW " + viewName + " AS " + query;
        if (!hasBehavior(SUPPORTS_CREATE_FEDERATED_MATERIALIZED_VIEW)) {
            // Note: the expected message may need to be updated when a connector supports materialized views, but not federated.
            assertQueryFails(create, "This connector does not support creating materialized views");
            return;
        }

        Session defaultSession = getSession();
        Session futureSession = Session.builder(defaultSession)
                // This gets ignored: .setStart(...)
                .setSystemProperty(TESTING_SESSION_TIME, Instant.now().plus(1, ChronoUnit.DAYS).toString())
                .build();

        PlanMatchPattern readFromBaseTables = anyTree(
                node(AggregationNode.class, // final
                        anyTree(// exchanges
                                node(AggregationNode.class, // partial
                                        anyTree(tableScan("tables"))))));
        PlanMatchPattern readFromStorageTable = node(OutputNode.class, node(TableScanNode.class));

        AtomicReference<List<String>> mockListing = new AtomicReference<>(List.of("first_table"));
        String initialResults = "VALUES (VARCHAR 'first_table', BIGINT '1')";
        withMockTableListing(
                mockSchemaForListing,
                connectorSession -> List.copyOf(mockListing.get()),
                () -> {
                    assertUpdate(create);

                    // The MV is initially not fresh
                    assertThat(getMaterializedViewFreshness(viewName)).isEqualTo(STALE);
                    assertThat(getMaterializedViewLastFreshTime(viewName)).isEmpty();
                    assertThat(query(defaultSession, "TABLE " + viewName)).hasPlan(readFromBaseTables).matches(initialResults);
                    assertThat(query(futureSession, "TABLE " + viewName)).hasPlan(readFromBaseTables).matches(initialResults);

                    ZonedDateTime beforeRefresh = ZonedDateTime.now();
                    assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 1);
                    ZonedDateTime afterRefresh = ZonedDateTime.now();

                    // Even right after the REFRESH, it's unknown that the view is FRESH
                    assertThat(getMaterializedViewFreshness(viewName)).isEqualTo(UNKNOWN);
                    assertThat(getMaterializedViewLastFreshTime(viewName))
                            .get(ZONED_DATE_TIME).isBetween(beforeRefresh, afterRefresh);
                    assertThat(query(defaultSession, "TABLE " + viewName)).hasPlan(readFromStorageTable).matches(initialResults);
                    assertThat(query(futureSession, "TABLE " + viewName)).hasPlan(readFromStorageTable).matches(initialResults);

                    // Change underlying state
                    mockListing.set(List.of("first_table", "second_table"));
                    String updatedResults = "VALUES (VARCHAR 'first_table', BIGINT '1'), ('second_table', 1)";

                    // The materialization is stale now
                    assertThat(getMaterializedViewFreshness(viewName)).isEqualTo(UNKNOWN);
                    assertThat(getMaterializedViewLastFreshTime(viewName))
                            .get(ZONED_DATE_TIME).isBetween(beforeRefresh, afterRefresh);
                    assertThat(query(defaultSession, "TABLE " + viewName)).hasPlan(readFromStorageTable).matches(initialResults);
                    assertThat(query(futureSession, "TABLE " + viewName)).hasPlan(readFromStorageTable).matches(initialResults);

                    ZonedDateTime beforeSecondRefresh = ZonedDateTime.now();
                    assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 2);
                    ZonedDateTime afterSecondRefresh = ZonedDateTime.now();

                    // Even right after the REFRESH, it's unknown that the view is FRESH
                    assertThat(getMaterializedViewFreshness(viewName)).isEqualTo(UNKNOWN);
                    assertThat(getMaterializedViewLastFreshTime(viewName))
                            .get(ZONED_DATE_TIME).isBetween(beforeSecondRefresh, afterSecondRefresh);
                    assertThat(query(defaultSession, "TABLE " + viewName)).hasPlan(readFromStorageTable).matches(updatedResults);
                    assertThat(query(futureSession, "TABLE " + viewName)).hasPlan(readFromStorageTable).matches(updatedResults);

                    assertUpdate("DROP MATERIALIZED VIEW " + viewName);
                });
    }

    @Test
    public void testFederatedMaterializedViewWithGracePeriod()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_FEDERATED_MATERIALIZED_VIEW));
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW));

        String viewName = "test_federated_mv_grace_period_" + randomNameSuffix();
        String mockSchemaForListing = "mock_schema_for_listing_" + randomNameSuffix();

        String query = "" +
                "SELECT table_name, count(*) AS c FROM mock_dynamic_listing.information_schema.tables " +
                "WHERE table_schema = '" + mockSchemaForListing + "' " +
                "GROUP BY table_name"; // GROUP BY so that it is easy to distinguish between inlined view and reading materialization

        Session defaultSession = getSession();
        Session futureSession = Session.builder(defaultSession)
                // This gets ignored: .setStart(...)
                .setSystemProperty(TESTING_SESSION_TIME, Instant.now().plus(1, ChronoUnit.DAYS).toString())
                .build();

        PlanMatchPattern readFromBaseTables = anyTree(
                node(AggregationNode.class, // final
                        anyTree(// exchanges
                                node(AggregationNode.class, // partial
                                        anyTree(tableScan("tables"))))));
        PlanMatchPattern readFromStorageTable = node(OutputNode.class, node(TableScanNode.class));

        AtomicReference<List<String>> mockListing = new AtomicReference<>(List.of("first_table"));
        String initialResults = "VALUES (VARCHAR 'first_table', BIGINT '1')";
        withMockTableListing(
                mockSchemaForListing,
                connectorSession -> List.copyOf(mockListing.get()),
                () -> {
                    assertUpdate("CREATE MATERIALIZED VIEW " + viewName + " " +
                            "GRACE PERIOD INTERVAL '1' HOUR " +
                            " AS " + query);

                    // The MV is initially not fresh
                    assertThat(getMaterializedViewFreshness(viewName)).isEqualTo(STALE);
                    assertThat(getMaterializedViewLastFreshTime(viewName)).isEmpty();
                    assertThat(query(defaultSession, "TABLE " + viewName)).hasPlan(readFromBaseTables).matches(initialResults);
                    assertThat(query(futureSession, "TABLE " + viewName)).hasPlan(readFromBaseTables).matches(initialResults);

                    ZonedDateTime beforeRefresh = ZonedDateTime.now();
                    assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 1);
                    ZonedDateTime afterRefresh = ZonedDateTime.now();

                    // Even right after the REFRESH, it's unknown that the view is FRESH
                    assertThat(getMaterializedViewFreshness(viewName)).isEqualTo(UNKNOWN);
                    assertThat(getMaterializedViewLastFreshTime(viewName))
                            .get(ZONED_DATE_TIME).isBetween(beforeRefresh, afterRefresh);
                    assertThat(query(defaultSession, "TABLE " + viewName)).hasPlan(readFromStorageTable).matches(initialResults);
                    assertThat(query(futureSession, "TABLE " + viewName)).hasPlan(readFromBaseTables).matches(initialResults);

                    // Change underlying state
                    mockListing.set(List.of("first_table", "second_table"));
                    String updatedResults = "VALUES (VARCHAR 'first_table', BIGINT '1'), ('second_table', 1)";

                    // The materialization is stale now
                    assertThat(getMaterializedViewFreshness(viewName)).isEqualTo(UNKNOWN);
                    assertThat(getMaterializedViewLastFreshTime(viewName))
                            .get(ZONED_DATE_TIME).isBetween(beforeRefresh, afterRefresh);
                    assertThat(query(defaultSession, "TABLE " + viewName)).hasPlan(readFromStorageTable).matches(initialResults);
                    assertThat(query(futureSession, "TABLE " + viewName)).hasPlan(readFromBaseTables).matches(updatedResults);

                    ZonedDateTime beforeSecondRefresh = ZonedDateTime.now();
                    assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 2);
                    ZonedDateTime afterSecondRefresh = ZonedDateTime.now();

                    // Even right after the REFRESH, it's unknown that the view is FRESH
                    assertThat(getMaterializedViewFreshness(viewName)).isEqualTo(UNKNOWN);
                    assertThat(getMaterializedViewLastFreshTime(viewName))
                            .get(ZONED_DATE_TIME).isBetween(beforeSecondRefresh, afterSecondRefresh);
                    assertThat(query(defaultSession, "TABLE " + viewName)).hasPlan(readFromStorageTable).matches(updatedResults);
                    assertThat(query(futureSession, "TABLE " + viewName)).hasPlan(readFromBaseTables).matches(updatedResults);

                    assertUpdate("DROP MATERIALIZED VIEW " + viewName);
                });
    }

    @Test
    public void testMaterializedViewBaseTableGone()
    {
        testMaterializedViewBaseTableGone(true);
        testMaterializedViewBaseTableGone(false);
    }

    private void testMaterializedViewBaseTableGone(boolean initialized)
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW));

        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String viewName = "mv_base_table_missing_" + randomNameSuffix();
        String baseTable = "mv_base_table_missing_the_table_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + baseTable + " AS SELECT 1 a", 1);
        assertUpdate("CREATE MATERIALIZED VIEW " + viewName + " AS SELECT * FROM " + baseTable);
        if (initialized) {
            assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 1);
        }
        assertUpdate("DROP TABLE " + baseTable);
        assertQueryFails(
                "TABLE " + viewName,
                "line 1:1: Failed analyzing stored view '%1$s\\.%2$s\\.%3$s': line 3:3: Table '%1$s\\.%2$s\\.%4$s' does not exist".formatted(catalog, schema, viewName, baseTable));
        assertUpdate("DROP MATERIALIZED VIEW " + viewName);
    }

    private MaterializedViewFreshness.Freshness getMaterializedViewFreshness(String materializedViewName)
    {
        String freshness = (String) computeScalar(
                "SELECT freshness FROM system.metadata.materialized_views " +
                        "WHERE catalog_name = CURRENT_CATALOG " +
                        "AND schema_name = CURRENT_SCHEMA " +
                        "AND name = '" + materializedViewName + "'");
        return MaterializedViewFreshness.Freshness.valueOf(freshness);
    }

    private Optional<ZonedDateTime> getMaterializedViewLastFreshTime(String materializedViewName)
    {
        ZonedDateTime lastFreshTime = (ZonedDateTime) computeScalar(
                "SELECT last_fresh_time FROM system.metadata.materialized_views " +
                        "WHERE catalog_name = CURRENT_CATALOG " +
                        "AND schema_name = CURRENT_SCHEMA " +
                        "AND name = '" + materializedViewName + "'");
        return Optional.ofNullable(lastFreshTime);
    }

    @Test
    public void testColumnCommentMaterializedView()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW));

        String viewName = "test_materialized_view_" + randomNameSuffix();
        if (!hasBehavior(SUPPORTS_COMMENT_ON_MATERIALIZED_VIEW_COLUMN)) {
            assertUpdate("CREATE MATERIALIZED VIEW " + viewName + " AS SELECT * FROM nation");
            assertQueryFails("COMMENT ON COLUMN " + viewName + ".regionkey IS 'new region key comment'", "This connector does not support setting materialized view column comments");
            assertUpdate("DROP MATERIALIZED VIEW " + viewName);
            return;
        }

        assertUpdate("CREATE MATERIALIZED VIEW " + viewName + " AS SELECT * FROM nation");
        try {
            assertUpdate("COMMENT ON COLUMN " + viewName + ".name IS 'new comment'");
            assertThat(getColumnComment(viewName, "name")).isEqualTo("new comment");

            // comment deleted
            assertUpdate("COMMENT ON COLUMN " + viewName + ".name IS NULL");
            assertThat(getColumnComment(viewName, "name")).isEqualTo(null);

            // comment set to non-empty value before verifying setting empty comment
            assertUpdate("COMMENT ON COLUMN " + viewName + ".name IS 'updated comment'");
            assertThat(getColumnComment(viewName, "name")).isEqualTo("updated comment");

            // refresh materialized view
            assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 25);
            assertThat(getColumnComment(viewName, "name")).isEqualTo("updated comment");

            // comment set to empty
            assertUpdate("COMMENT ON COLUMN " + viewName + ".name IS ''");
            assertThat(getColumnComment(viewName, "name")).isEmpty();
        }
        finally {
            assertUpdate("DROP MATERIALIZED VIEW " + viewName);
        }
    }

    @Test
    public void testCompatibleTypeChangeForView()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        String tableName = "test_table_" + randomNameSuffix();
        String viewName = "test_view_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 'abcdefg' a", 1);
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT a FROM " + tableName);

        assertQuery("SELECT * FROM " + viewName, "VALUES 'abcdefg'");

        // replace table with a version that's implicitly coercible to the previous one
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 'abc' a", 1);

        assertQuery("SELECT * FROM " + viewName, "VALUES 'abc'");

        assertUpdate("DROP VIEW " + viewName);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCompatibleTypeChangeForView2()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        String tableName = "test_table_" + randomNameSuffix();
        String viewName = "test_view_" + randomNameSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT BIGINT '1' v", 1);
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);

        assertQuery("SELECT * FROM " + viewName, "VALUES 1");

        // replace table with a version that's implicitly coercible to the previous one
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT INTEGER '1' v", 1);

        assertQuery("SELECT * FROM " + viewName + " WHERE v = 1", "VALUES 1");

        assertUpdate("DROP VIEW " + viewName);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testViewMetadata()
    {
        testViewMetadata("", "DEFINER");
        testViewMetadata(" SECURITY DEFINER", "DEFINER");
        testViewMetadata(" SECURITY INVOKER", "INVOKER");
    }

    private void testViewMetadata(String securityClauseInCreate, String securityClauseInShowCreate)
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));

        String viewName = "meta_test_view_" + randomNameSuffix();

        @Language("SQL") String query = "SELECT BIGINT '123' x, 'foo' y";
        assertUpdate("CREATE VIEW " + viewName + securityClauseInCreate + " AS " + query);

        // test INFORMATION_SCHEMA.TABLES
        MaterializedResult actual = computeActual(format(
                "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = '%s'",
                getSession().getSchema().get()));

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("customer", "BASE TABLE")
                .row(viewName, "VIEW")
                .row("nation", "BASE TABLE")
                .row("orders", "BASE TABLE")
                .row("region", "BASE TABLE")
                .build();

        assertContains(actual, expected);

        // test SHOW TABLES
        actual = computeActual("SHOW TABLES");

        MaterializedResult.Builder builder = resultBuilder(getSession(), actual.getTypes());
        for (MaterializedRow row : expected.getMaterializedRows()) {
            builder.row(row.getField(0));
        }
        expected = builder.build();

        assertContains(actual, expected);

        // test INFORMATION_SCHEMA.VIEWS
        actual = computeActual(format(
                "SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = '%s'",
                getSession().getSchema().get()));

        expected = resultBuilder(getSession(), actual.getTypes())
                .row(viewName, formatSqlText(query))
                .build();

        assertContains(actual, expected);

        // test SHOW COLUMNS
        assertThat(query("SHOW COLUMNS FROM " + viewName))
                .result().matches(resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                        .row("x", "bigint", "", "")
                        .row("y", "varchar(3)", "", "")
                        .build());

        // test SHOW CREATE VIEW
        String expectedSql = formatSqlText(format(
                "CREATE VIEW %s.%s.%s SECURITY %s AS %s",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                viewName,
                securityClauseInShowCreate,
                query)).trim();

        actual = computeActual("SHOW CREATE VIEW " + viewName);

        assertThat(getOnlyElement(actual.getOnlyColumnAsSet())).isEqualTo(expectedSql);

        actual = computeActual(format("SHOW CREATE VIEW %s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), viewName));

        assertThat(getOnlyElement(actual.getOnlyColumnAsSet())).isEqualTo(expectedSql);

        assertUpdate("DROP VIEW " + viewName);
    }

    @Test
    public void testShowCreateView()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_VIEW));
        checkState(getSession().getCatalog().isPresent(), "catalog is not set");
        checkState(getSession().getSchema().isPresent(), "schema is not set");

        String viewName = "test_show_create_view" + randomNameSuffix();
        assertUpdate("DROP VIEW IF EXISTS " + viewName);
        String ddl = format(
                "CREATE VIEW %s.%s.%s SECURITY DEFINER AS\n" +
                        "SELECT *\n" +
                        "FROM\n" +
                        "  (\n" +
                        " VALUES \n" +
                        "     ROW (1, 'one')\n" +
                        "   , ROW (2, 't')\n" +
                        ")  t (col1, col2)",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                viewName);
        assertUpdate(ddl);

        assertThat(computeScalar("SHOW CREATE VIEW " + viewName)).isEqualTo(ddl);

        assertUpdate("DROP VIEW " + viewName);
    }

    @Test
    public void testRenameMaterializedView()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW));

        String schema = "rename_mv_test_" + randomNameSuffix();
        Session session = Session.builder(getSession())
                .setSchema(schema)
                .build();
        assertUpdate(createSchemaSql(schema));

        QualifiedObjectName originalMaterializedView = new QualifiedObjectName(
                session.getCatalog().orElseThrow(),
                session.getSchema().orElseThrow(),
                "test_materialized_view_rename_" + randomNameSuffix());

        createTestingMaterializedView(originalMaterializedView, Optional.empty());

        String renamedMaterializedView = "test_materialized_view_rename_new_" + randomNameSuffix();
        if (!hasBehavior(SUPPORTS_RENAME_MATERIALIZED_VIEW)) {
            assertQueryFails(session, "ALTER MATERIALIZED VIEW " + originalMaterializedView + " RENAME TO " + renamedMaterializedView, "This connector does not support renaming materialized views");
            assertUpdate(session, "DROP MATERIALIZED VIEW " + originalMaterializedView);
            return;
        }

        // simple rename
        assertUpdate(session, "ALTER MATERIALIZED VIEW " + originalMaterializedView + " RENAME TO " + renamedMaterializedView);
        assertTestingMaterializedViewQuery(schema, renamedMaterializedView);
        // verify new name in the system.metadata.materialized_views
        assertQuery(session, "SELECT catalog_name, schema_name FROM system.metadata.materialized_views WHERE name = '" + renamedMaterializedView + "'",
                format("VALUES ('%s', '%s')", originalMaterializedView.catalogName(), originalMaterializedView.schemaName()));
        assertQueryReturnsEmptyResult(session, listMaterializedViewsSql("name = '" + originalMaterializedView.objectName() + "'"));

        // rename with IF EXISTS on existing materialized view
        String testExistsMaterializedViewName = "test_materialized_view_rename_exists_" + randomNameSuffix();
        assertUpdate(session, "ALTER MATERIALIZED VIEW IF EXISTS " + renamedMaterializedView + " RENAME TO " + testExistsMaterializedViewName);
        assertTestingMaterializedViewQuery(schema, testExistsMaterializedViewName);

        // rename with upper-case, not delimited identifier
        String uppercaseName = "TEST_MATERIALIZED_VIEW_RENAME_UPPERCASE_" + randomNameSuffix();
        assertUpdate(session, "ALTER MATERIALIZED VIEW " + testExistsMaterializedViewName + " RENAME TO " + uppercaseName);
        assertTestingMaterializedViewQuery(schema, uppercaseName.toLowerCase(ENGLISH)); // Ensure select allows for lower-case, not delimited identifier

        String otherSchema = "rename_mv_other_schema_" + randomNameSuffix();
        assertUpdate(createSchemaSql(otherSchema));
        if (hasBehavior(SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS)) {
            assertUpdate(session, "ALTER MATERIALIZED VIEW " + uppercaseName + " RENAME TO " + otherSchema + "." + originalMaterializedView.objectName());
            assertTestingMaterializedViewQuery(otherSchema, originalMaterializedView.objectName());

            assertUpdate(session, "DROP MATERIALIZED VIEW " + otherSchema + "." + originalMaterializedView.objectName());
        }
        else {
            assertQueryFails(
                    session,
                    "ALTER MATERIALIZED VIEW " + uppercaseName + " RENAME TO " + otherSchema + "." + originalMaterializedView.objectName(),
                    "Materialized View rename across schemas is not supported");
            assertUpdate(session, "DROP MATERIALIZED VIEW " + uppercaseName);
        }

        assertThat(getQueryRunner().tableExists(session, originalMaterializedView.toString())).isFalse();
        assertThat(getQueryRunner().tableExists(session, renamedMaterializedView)).isFalse();
        assertThat(getQueryRunner().tableExists(session, testExistsMaterializedViewName)).isFalse();

        // rename with IF EXISTS on NOT existing materialized view
        assertUpdate(session, "ALTER TABLE IF EXISTS " + originalMaterializedView + " RENAME TO " + renamedMaterializedView);
        assertQueryReturnsEmptyResult(session, listMaterializedViewsSql("name = '" + originalMaterializedView.objectName() + "'"));
        assertQueryReturnsEmptyResult(session, listMaterializedViewsSql("name = '" + renamedMaterializedView + "'"));
    }

    private void assertTestingMaterializedViewQuery(String schema, String materializedViewName)
    {
        assertThat(query("SELECT * FROM " + schema + "." + materializedViewName))
                .skippingTypesCheck()
                .matches("SELECT * FROM nation");
    }

    private void createTestingMaterializedView(QualifiedObjectName view, Optional<String> comment)
    {
        assertUpdate(format(
                "CREATE MATERIALIZED VIEW %s %s AS SELECT * FROM nation",
                view,
                comment.map(c -> format("COMMENT '%s'", c)).orElse("")));
    }

    private String getTestingMaterializedViewsResultRow(QualifiedObjectName materializedView, String comment)
    {
        return format(
                "VALUES ('%s', '%s', '%s', '%s', 'SELECT *\nFROM\n  nation\n')",
                materializedView.catalogName(),
                materializedView.schemaName(),
                materializedView.objectName(),
                comment);
    }

    private String getTestingMaterializedViewsResultRows(
            QualifiedObjectName materializedView,
            QualifiedObjectName otherMaterializedView)
    {
        String viewDefinitionSql = "SELECT *\nFROM\n  nation\n";

        return format(
                "VALUES ('%s', '%s', '%s', '', '%s')," +
                        "('%s', '%s', '%s', 'sarcastic comment', '%s')",
                materializedView.catalogName(),
                materializedView.schemaName(),
                materializedView.objectName(),
                viewDefinitionSql,
                otherMaterializedView.catalogName(),
                otherMaterializedView.schemaName(),
                otherMaterializedView.objectName(),
                viewDefinitionSql);
    }

    private String listMaterializedViewsSql(String... filterClauses)
    {
        StringBuilder sql = new StringBuilder("SELECT" +
                "   catalog_name," +
                "   schema_name," +
                "   name," +
                "   comment," +
                "   definition " +
                "FROM system.metadata.materialized_views " +
                "WHERE true");

        for (String filterClause : filterClauses) {
            sql.append(" AND ").append(filterClause);
        }

        return sql.toString();
    }

    @Test
    public void testViewAndMaterializedViewTogether()
    {
        if (!hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW) || !hasBehavior(SUPPORTS_CREATE_VIEW)) {
            return;
        }
        // Validate that it is possible to have views and materialized views defined at the same time and both are operational

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();

        String regularViewName = "test_views_together_normal_" + randomNameSuffix();
        assertUpdate("CREATE VIEW " + regularViewName + " AS SELECT * FROM region");

        String materializedViewName = "test_views_together_materialized_" + randomNameSuffix();
        assertUpdate("CREATE MATERIALIZED VIEW " + materializedViewName + " AS SELECT * FROM nation");

        // only the regular view should be accessible via information_schema.views
        assertThat(query("SELECT table_name FROM information_schema.views WHERE table_schema = '" + schemaName + "' AND table_name IN ('" + regularViewName + "', '" + materializedViewName + "')"))
                .matches("VALUES VARCHAR '" + regularViewName + "'");
        assertThat(computeActual("SELECT table_name FROM information_schema.views WHERE table_schema = '" + schemaName + "'").getOnlyColumnAsSet())
                .contains(regularViewName)
                .doesNotContain(materializedViewName);

        // only the materialized view should be accessible via system.metadata.materialized_view
        assertThat(computeActual("SELECT name FROM system.metadata.materialized_views WHERE catalog_name = '" + catalogName + "' AND schema_name = '" + schemaName + "'").getOnlyColumnAsSet())
                .doesNotContain(regularViewName)
                .contains(materializedViewName);

        // check we can query from both
        assertThat(query("SELECT * FROM " + regularViewName)).containsAll("SELECT * FROM region");
        assertThat(query("SELECT * FROM " + materializedViewName)).containsAll("SELECT * FROM nation");

        assertUpdate("DROP VIEW " + regularViewName);
        assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
    }

    @Test
    public void testExplainAnalyze()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT * FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT x + y FROM (" +
                        "   SELECT orderdate, COUNT(*) x FROM orders GROUP BY orderdate) a JOIN (" +
                        "   SELECT orderdate, COUNT(*) y FROM orders GROUP BY orderdate) b ON a.orderdate = b.orderdate");
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk UNION ALL SELECT sum(orderkey), clerk FROM orders GROUP BY clerk");

        assertExplainAnalyze("EXPLAIN ANALYZE SHOW COLUMNS FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE EXPLAIN SELECT count(*) FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE EXPLAIN ANALYZE SELECT count(*) FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW FUNCTIONS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW TABLES");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW SCHEMAS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW CATALOGS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW SESSION");
    }

    @Test
    public void testExplainAnalyzeVerbose()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT * FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT rank() OVER (PARTITION BY orderkey ORDER BY clerk DESC) FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT rank() OVER (PARTITION BY orderkey ORDER BY clerk DESC) FROM orders WHERE orderkey < 0");
    }

    @Test
    public void testTableSampleSystem()
    {
        MaterializedResult fullSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (0)");
        MaterializedResult randomSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (50)");
        MaterializedResult all = computeActual("SELECT orderkey FROM orders");

        assertContains(all, fullSample);
        assertThat(emptySample.getMaterializedRows()).isEmpty();
        assertThat(all.getMaterializedRows().size() >= randomSample.getMaterializedRows().size()).isTrue();
    }

    @Test
    public void testTableSampleWithFiltering()
    {
        MaterializedResult emptySample = computeActual("SELECT DISTINCT orderkey, orderdate FROM orders TABLESAMPLE SYSTEM (99) WHERE orderkey BETWEEN 0 AND 0");
        MaterializedResult halfSample = computeActual("SELECT DISTINCT orderkey, orderdate FROM orders TABLESAMPLE SYSTEM (50) WHERE orderkey BETWEEN 0 AND 9999999999");
        MaterializedResult all = computeActual("SELECT orderkey, orderdate FROM orders");

        assertThat(emptySample.getMaterializedRows()).isEmpty();
        // Assertions need to be loose here because SYSTEM sampling random selects data on split boundaries. In this case either all the data will be selected, or
        // none of it. Sampling with a 100% ratio is ignored, so that also cannot be used to guarantee results.
        assertThat(all.getMaterializedRows().size() >= halfSample.getMaterializedRows().size()).isTrue();
    }

    @Test
    public void testShowCreateTable()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        assertThat(computeScalar("SHOW CREATE TABLE orders"))
                // If the connector reports additional column properties, the expected value needs to be adjusted in the test subclass
                .isEqualTo(format(
                        """
                        CREATE TABLE %s.%s.orders (
                           orderkey bigint,
                           custkey bigint,
                           orderstatus varchar(1),
                           totalprice double,
                           orderdate date,
                           orderpriority varchar(15),
                           clerk varchar(15),
                           shippriority integer,
                           comment varchar(79)
                        )\
                        """,
                        catalog,
                        schema));
    }

    @Test
    public void testSelectInformationSchemaTables()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String schemaPattern = schema.replaceAll("^.", "_");

        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema + "' AND table_name = 'orders'", "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema LIKE '" + schema + "' AND table_name LIKE '%rders'", "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '%rders'", "VALUES 'orders'");
        assertQuery(
                "SELECT table_name FROM information_schema.tables " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema LIKE '" + schema + "' AND table_name LIKE '%orders'",
                "VALUES 'orders'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_catalog = 'something_else'", "SELECT '' WHERE false");

        assertQuery(
                "SELECT DISTINCT table_name FROM information_schema.tables WHERE table_schema = 'information_schema' OR rand() = 42 ORDER BY 1",
                "VALUES " +
                        "('applicable_roles'), " +
                        "('columns'), " +
                        "('enabled_roles'), " +
                        "('roles'), " +
                        "('schemata'), " +
                        "('table_privileges'), " +
                        "('tables'), " +
                        "('views')");
    }

    @Test
    public void testSelectInformationSchemaColumns()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String schemaPattern = schema.replaceAll(".$", "_");

        String ordersTableWithColumns = getOrdersTableWithColumns();

        assertQuery("SELECT table_schema FROM information_schema.columns WHERE table_schema = '" + schema + "' GROUP BY table_schema", "VALUES '" + schema + "'");
        assertQuery("SELECT table_name FROM information_schema.columns WHERE table_name = 'orders' GROUP BY table_name", "VALUES 'orders'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'orders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%rders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '_rder_'", ordersTableWithColumns);
        assertThat(query(
                "SELECT table_name, column_name FROM information_schema.columns " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '%orders%'"))
                .skippingTypesCheck()
                .containsAll(ordersTableWithColumns);

        assertQuerySucceeds("SELECT * FROM information_schema.columns");
        assertQuery("SELECT DISTINCT table_name, column_name FROM information_schema.columns WHERE table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "'");
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_name LIKE '%'");
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_catalog = 'something_else'", "SELECT '' WHERE false");

        assertQuery(
                "SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema = 'information_schema' OR rand() = 42 ORDER BY 1",
                "VALUES " +
                        "('applicable_roles'), " +
                        "('columns'), " +
                        "('enabled_roles'), " +
                        "('roles'), " +
                        "('schemata'), " +
                        "('table_privileges'), " +
                        "('tables'), " +
                        "('views')");
    }

    protected @Language("SQL") String getOrdersTableWithColumns()
    {
        return
               """
               VALUES
               ('orders', 'orderkey'),
               ('orders', 'custkey'),
               ('orders', 'orderstatus'),
               ('orders', 'totalprice'),
               ('orders', 'orderdate'),
               ('orders', 'orderpriority'),
               ('orders', 'clerk'),
               ('orders', 'shippriority'),
               ('orders', 'comment')
               """;
    }

    @Test
    public void testShowCreateInformationSchema()
    {
        assertThat(query("SHOW CREATE SCHEMA information_schema"))
                .skippingTypesCheck()
                .matches(format("VALUES 'CREATE SCHEMA %s.information_schema'", getSession().getCatalog().orElseThrow()));
    }

    @Test
    public void testShowCreateInformationSchemaTable()
    {
        assertQueryFails("SHOW CREATE VIEW information_schema.schemata", "line 1:1: Relation '\\w+.information_schema.schemata' is a table, not a view");
        assertQueryFails("SHOW CREATE MATERIALIZED VIEW information_schema.schemata", "line 1:1: Relation '\\w+.information_schema.schemata' is a table, not a materialized view");

        assertThat((String) computeScalar("SHOW CREATE TABLE information_schema.schemata"))
                .isEqualTo("CREATE TABLE " + getSession().getCatalog().orElseThrow() + ".information_schema.schemata (\n" +
                        "   catalog_name varchar,\n" +
                        "   schema_name varchar\n" +
                        ")");
    }

    @Test
    public void testRollback()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MULTI_STATEMENT_WRITES));

        String table = "test_rollback_" + randomNameSuffix();
        computeActual(format("CREATE TABLE %s (x int)", table));

        assertThatThrownBy(() ->
                inTransaction(session -> {
                    assertUpdate(session, format("INSERT INTO %s VALUES (42)", table), 1);
                    throw new RollbackException();
                }))
                .isInstanceOf(RollbackException.class);

        assertQuery(format("SELECT count(*) FROM %s", table), "SELECT 0");
    }

    private static class RollbackException
            extends RuntimeException {}

    @Test
    public void testWriteNotAllowedInTransaction()
    {
        skipTestUnless(!hasBehavior(SUPPORTS_MULTI_STATEMENT_WRITES));

        assertWriteNotAllowedInTransaction(SUPPORTS_CREATE_SCHEMA, "CREATE SCHEMA write_not_allowed");
        assertWriteNotAllowedInTransaction(SUPPORTS_CREATE_TABLE, "CREATE TABLE write_not_allowed (x int)");
        assertWriteNotAllowedInTransaction(SUPPORTS_CREATE_TABLE, "DROP TABLE region");
        assertWriteNotAllowedInTransaction(SUPPORTS_CREATE_TABLE_WITH_DATA, "CREATE TABLE write_not_allowed AS SELECT * FROM region");
        assertWriteNotAllowedInTransaction(SUPPORTS_CREATE_VIEW, "CREATE VIEW write_not_allowed AS SELECT * FROM region");
        assertWriteNotAllowedInTransaction(SUPPORTS_CREATE_MATERIALIZED_VIEW, "CREATE MATERIALIZED VIEW write_not_allowed AS SELECT * FROM region");
        assertWriteNotAllowedInTransaction(SUPPORTS_RENAME_TABLE, "ALTER TABLE region RENAME TO region_name");
        assertWriteNotAllowedInTransaction(SUPPORTS_INSERT, "INSERT INTO region (regionkey) VALUES (123)");
        assertWriteNotAllowedInTransaction(SUPPORTS_DELETE, "DELETE FROM region WHERE regionkey = 123");

        // REFRESH MATERIALIZED VIEW is tested in testMaterializedView
    }

    protected void assertWriteNotAllowedInTransaction(TestingConnectorBehavior behavior, @Language("SQL") String sql)
    {
        if (hasBehavior(behavior)) {
            assertThatThrownBy(() -> inTransaction(session -> computeActual(session, sql)))
                    .hasMessageMatching("Catalog only supports writes using autocommit: \\w+");
        }
    }

    @Test
    public void testRenameSchema()
    {
        if (!hasBehavior(SUPPORTS_RENAME_SCHEMA)) {
            String schemaName = getSession().getSchema().orElseThrow();
            assertQueryFails(
                    format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomNameSuffix()),
                    "This connector does not support renaming schemas");
            return;
        }

        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            abort("Skipping as connector does not support CREATE SCHEMA");
        }

        String schemaName = "test_rename_schema_" + randomNameSuffix();
        try {
            assertUpdate(createSchemaSql(schemaName));
            assertUpdate("ALTER SCHEMA " + schemaName + " RENAME TO " + schemaName + "_renamed");
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet())
                    .doesNotContain(schemaName)
                    .contains(schemaName + "_renamed");
        }
        finally {
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName + "_renamed");
        }
    }

    @Test
    public void testDropSchemaCascade()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_SCHEMA));

        if (!hasBehavior(SUPPORTS_DROP_SCHEMA_CASCADE)) {
            String schemaName = "test_drop_schema_cascade_" + randomNameSuffix();
            assertUpdate(createSchemaSql(schemaName));
            assertQueryFails(
                    "DROP SCHEMA " + schemaName + " CASCADE",
                    "This connector does not support dropping schemas with CASCADE option");
            assertUpdate("DROP SCHEMA " + schemaName);
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) || hasBehavior(SUPPORTS_CREATE_VIEW) || hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW));

        String schemaName = "test_drop_schema_cascade_" + randomNameSuffix();
        String tableName = "test_table" + randomNameSuffix();
        String viewName = "test_view" + randomNameSuffix();
        String materializedViewName = "test_materialized_view" + randomNameSuffix();
        try {
            assertUpdate(createSchemaSql(schemaName));
            if (hasBehavior(SUPPORTS_CREATE_TABLE)) {
                assertUpdate("CREATE TABLE " + schemaName + "." + tableName + "(a INT)");
            }
            if (hasBehavior(SUPPORTS_CREATE_VIEW)) {
                assertUpdate("CREATE VIEW " + schemaName + "." + viewName + " AS SELECT 1 a");
            }
            if (hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW)) {
                assertUpdate("CREATE MATERIALIZED VIEW " + schemaName + "." + materializedViewName + " AS SELECT 1 a");
            }

            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);

            assertUpdate("DROP SCHEMA " + schemaName + " CASCADE");
            assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schemaName);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + "." + tableName);
            assertUpdate("DROP VIEW IF EXISTS " + schemaName + "." + viewName);
            assertUpdate("DROP MATERIALIZED VIEW IF EXISTS " + schemaName + "." + materializedViewName);
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    @Test
    public void testAddColumn()
    {
        if (!hasBehavior(SUPPORTS_ADD_COLUMN)) {
            assertQueryFails("ALTER TABLE nation ADD COLUMN test_add_column bigint", "This connector does not support adding columns");
            return;
        }

        String tableName;
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_column_", tableDefinitionForAddColumn())) {
            tableName = table.getName();
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'first'", 1);
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN x bigint", ".* Column 'x' already exists");
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN X bigint", ".* Column 'X' already exists");
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN q bad_type", ".* Unknown type 'bad_type' for column 'q'");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN a varchar(50)");
            // Verify table state after adding a column, but before inserting anything to it
            assertQuery(
                    "SELECT * FROM " + table.getName(),
                    "VALUES ('first', NULL)");
            assertQuery(
                    "SELECT * FROM " + table.getName() + " WHERE a IS NULL",
                    "VALUES ('first', NULL)");
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'second', 'xxx'", 1);
            assertQuery(
                    "SELECT x, a FROM " + table.getName(),
                    "VALUES ('first', NULL), ('second', 'xxx')");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN b double");
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'third', 'yyy', 33.3E0", 1);
            assertQuery(
                    "SELECT x, a, b FROM " + table.getName(),
                    "VALUES ('first', NULL, NULL), ('second', 'xxx', NULL), ('third', 'yyy', 33.3)");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN IF NOT EXISTS c varchar(50)");
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN IF NOT EXISTS c varchar(50)");
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'fourth', 'zzz', 55.3E0, 'newColumn'", 1);
            assertQuery(
                    "SELECT x, a, b, c FROM " + table.getName(),
                    "VALUES ('first', NULL, NULL, NULL), ('second', 'xxx', NULL, NULL), ('third', 'yyy', 33.3, NULL), ('fourth', 'zzz', 55.3, 'newColumn')");
        }

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN x bigint");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN IF NOT EXISTS x bigint");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    /**
     * The table must have one column 'x' of varchar type.
     */
    protected String tableDefinitionForAddColumn()
    {
        return "(x VARCHAR)";
    }

    @Test
    public void testAddColumnWithComment()
    {
        if (!hasBehavior(SUPPORTS_ADD_COLUMN)) {
            // Covered by testAddColumn
            return;
        }
        if (!hasBehavior(SUPPORTS_ADD_COLUMN_WITH_COMMENT)) {
            assertQueryFails("ALTER TABLE nation ADD COLUMN test_add_col_desc bigint COMMENT 'test column comment'", "This connector does not support adding columns with comments");
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_col_desc_", "(a_varchar varchar)")) {
            String tableName = table.getName();

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar COMMENT 'test new column comment'");
            assertThat(getColumnComment(tableName, "b_varchar")).isEqualTo("test new column comment");

            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN empty_comment varchar COMMENT ''");
            assertThat(getColumnComment(tableName, "empty_comment")).isEqualTo("");
        }
    }

    @Test
    public void testAddNotNullColumnToEmptyTable()
    {
        skipTestUnless(hasBehavior(SUPPORTS_ADD_COLUMN));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_nn_to_empty", "(a_varchar varchar)")) {
            String tableName = table.getName();
            String addNonNullColumn = "ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL";

            if (!hasBehavior(SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT)) {
                assertQueryFails(
                        addNonNullColumn,
                        hasBehavior(SUPPORTS_NOT_NULL_CONSTRAINT)
                                ? "This connector does not support adding not null columns"
                                : ".* Catalog '.*' does not support NOT NULL for column '.*'");
                return;
            }

            assertUpdate(addNonNullColumn);
            assertThat(columnIsNullable(tableName, "b_varchar")).isFalse();
            assertUpdate("INSERT INTO " + tableName + " VALUES ('a', 'b')", 1);
            assertThat(query("TABLE " + tableName))
                    .skippingTypesCheck()
                    .matches("VALUES ('a', 'b')");
        }
    }

    @Test
    public void testAddNotNullColumn()
    {
        skipTestUnless(hasBehavior(SUPPORTS_ADD_COLUMN_NOT_NULL_CONSTRAINT)); // covered by testAddNotNullColumnToEmptyTable

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_nn_col", "(a_varchar varchar)")) {
            String tableName = table.getName();

            assertUpdate("INSERT INTO " + tableName + " VALUES ('a')", 1);
            boolean success = false;
            try {
                assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b_varchar varchar NOT NULL");
                success = true;
            }
            catch (Throwable e) {
                verifyAddNotNullColumnToNonEmptyTableFailurePermissible(e);
            }
            if (success) {
                throw new AssertionError("Should fail to add not null column without a default value to a non-empty table");
            }
            assertThat(query("TABLE " + tableName))
                    .skippingTypesCheck()
                    .matches("VALUES 'a'");
        }
    }

    protected boolean columnIsNullable(String tableName, String columnName)
    {
        String isNullable = (String) computeScalar(
                "SELECT is_nullable FROM information_schema.columns WHERE " +
                        "table_schema = '" + getSession().getSchema().orElseThrow() + "' AND table_name = '" + tableName + "' AND column_name = '" + columnName + "'");
        return switch (requireNonNull(isNullable, "isNullable is null")) {
            case "YES" -> true;
            case "NO" -> false;
            default -> throw new IllegalStateException("Unrecognized is_nullable value: " + isNullable);
        };
    }

    protected void verifyAddNotNullColumnToNonEmptyTableFailurePermissible(Throwable e)
    {
        throw new AssertionError("Unexpected failure when adding not null column", e);
    }

    @Test
    public void testAddRowField()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_ROW_TYPE));

        if (!hasBehavior(SUPPORTS_ADD_FIELD)) {
            try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_field_", "AS SELECT CAST(row(1) AS row(x integer)) AS col")) {
                assertQueryFails(
                        "ALTER TABLE " + table.getName() + " ADD COLUMN col.y integer",
                        "This connector does not support adding fields");
            }
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute,
                "test_add_field_",
                "AS SELECT CAST(row(1, row(10)) AS row(a integer, b row(x integer))) AS col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a integer, b row(x integer))");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN col.c integer");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a integer, b row(x integer), c integer)");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(row(1, row(10), NULL) AS row(a integer, b row(x integer), c integer))");

            // Add a nested field
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN col.b.y integer");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a integer, b row(x integer, y integer), c integer)");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(row(1, row(10, NULL), NULL) AS row(a integer, b row(x integer, y integer), c integer))");

            // Specify existing fields with IF NOT EXISTS option
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN IF NOT EXISTS col.a varchar");
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN IF NOT EXISTS col.b.x varchar");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a integer, b row(x integer, y integer), c integer)");

            // Specify existing fields without IF NOT EXISTS option
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN col.a varchar", ".* Field 'a' already exists");
        }
    }

    @Test
    public void testAddRowFieldInArray()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_ROW_TYPE));

        if (!hasBehavior(SUPPORTS_ADD_FIELD_IN_ARRAY)) {
            try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_field_in_array_", "AS SELECT CAST(array[row(1)] AS array(row(x integer))) AS col")) {
                assertQueryFails(
                        "ALTER TABLE " + table.getName() + " ADD COLUMN col.element.y integer",
                        ".*does not support.*");
            }
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute,
                "test_add_field_in_array_",
                "AS SELECT CAST(array[row(1, row(10), array[row(11)])] AS array(row(a integer, b row(x integer), c array(row(v integer))))) AS col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(row(a integer, b row(x integer), c array(row(v integer))))");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN col.element.d integer");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(row(a integer, b row(x integer), c array(row(v integer)), d integer))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(array[row(1, row(10), array[row(11)], NULL)] AS array(row(a integer, b row(x integer), c array(row(v integer)), d integer)))");

            // Add a nested field
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN col.element.b.y integer");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(row(a integer, b row(x integer, y integer), c array(row(v integer)), d integer))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(array[row(1, row(10, NULL), array[row(11)], NULL)] AS array(row(a integer, b row(x integer, y integer), c array(row(v integer)), d integer)))");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN col.element.c.element.w integer");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(row(a integer, b row(x integer, y integer), c array(row(v integer, w integer)), d integer))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(array[row(1, row(10, NULL), array[row(11, NULL)], NULL)] AS array(row(a integer, b row(x integer, y integer), c array(row(v integer, w integer)), d integer)))");

            // Denote to array without 'element' designator
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " ADD COLUMN col.c.element.blah integer",
                    "\\QARRAY type should be denoted by 'element' in the path; found 'c'");

            // Specify existing fields with IF NOT EXISTS option
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN IF NOT EXISTS col.element.a varchar");
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN IF NOT EXISTS col.element.b.x varchar");
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN IF NOT EXISTS col.element.c.element.w varchar");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(row(a integer, b row(x integer, y integer), c array(row(v integer, w integer)), d integer))");

            // Specify existing fields without IF NOT EXISTS option
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN col.element.a varchar", ".* Field 'a' already exists");
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN col.element.b.x varchar", ".* Field 'x' already exists");
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN col.element.c.element.w varchar", ".* Field 'w' already exists");
        }

        // test row in array of arrays
        try (TestTable table = new TestTable(getQueryRunner()::execute,
                "test_add_field_in_array_nested_",
                "AS SELECT CAST(array[array[row(1, row(10), array[row(11)])]] AS array(array(row(a integer, b row(x integer), c array(row(v integer)))))) AS col")) {

            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(array(row(a integer, b row(x integer), c array(row(v integer)))))");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN col.element.element.d integer");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(array(row(a integer, b row(x integer), c array(row(v integer)), d integer)))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(array[array[row(1, row(10), array[row(11)], NULL)]] AS array(array(row(a integer, b row(x integer), c array(row(v integer)), d integer))))");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN col.element.element.b.y integer");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(array(row(a integer, b row(x integer, y integer), c array(row(v integer)), d integer)))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(array[array[row(1, row(10, NULL), array[row(11)], NULL)]] AS array(array(row(a integer, b row(x integer, y integer), c array(row(v integer)), d integer))))");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN col.element.element.c.element.w integer");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(array(row(a integer, b row(x integer, y integer), c array(row(v integer, w integer)), d integer)))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(array[array[row(1, row(10, NULL), array[row(11, NULL)], NULL)]] AS array(array(row(a integer, b row(x integer, y integer), c array(row(v integer, w integer)), d integer))))");
        }
    }

    @Test
    public void testDropColumn()
    {
        if (!hasBehavior(SUPPORTS_DROP_COLUMN)) {
            assertQueryFails("ALTER TABLE nation DROP COLUMN nationkey", "This connector does not support dropping columns");
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String tableName;
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_column_", "AS SELECT 123 x, 456 y, 111 a")) {
            tableName = table.getName();
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN x");
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS y");
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
            assertQueryFails("SELECT x FROM " + tableName, ".* Column 'x' cannot be resolved");
            assertQueryFails("SELECT y FROM " + tableName, ".* Column 'y' cannot be resolved");

            assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN a", ".* Cannot drop the only column in a table");
        }

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN notExistColumn");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    public void testDropRowField()
    {
        if (!hasBehavior(SUPPORTS_DROP_FIELD)) {
            if (!hasBehavior(SUPPORTS_DROP_COLUMN) || !hasBehavior(SUPPORTS_ROW_TYPE)) {
                return;
            }
            try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_field_", "AS SELECT CAST(row(1, 2) AS row(x integer, y integer)) AS col")) {
                assertQueryFails(
                        "ALTER TABLE " + table.getName() + " DROP COLUMN col.x",
                        "This connector does not support dropping fields");
            }
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute,
                "test_drop_field_",
                "AS SELECT CAST(row(1, 2, row(10, 20)) AS row(a integer, b integer, c row(x integer, y integer))) AS col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a integer, b integer, c row(x integer, y integer))");

            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.b");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a integer, c row(x integer, y integer))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(row(1, row(10, 20)) AS row(a integer, c row(x integer, y integer)))");

            // Drop a nested field
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.c.y");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a integer, c row(x integer))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(row(1, row(10)) AS row(a integer, c row(x integer)))");

            // Verify failure when trying to drop unique field in nested row type
            assertQueryFails("ALTER TABLE " + table.getName() + " DROP COLUMN col.c.x", ".* Cannot drop the only field in a row type");

            // Verify failure when trying to drop non-existing fields
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " DROP COLUMN col.c.non_existing",
                    "\\Qline 1:1: Cannot resolve field 'non_existing' within row(x integer) type when dropping [c, non_existing] in row(a integer, c row(x integer))");

            // Drop a row having fields
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.c");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a integer)");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(row(1) AS row(a integer))");

            // Specify non-existing fields with IF EXISTS option
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN IF EXISTS non_existing.a");
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN IF EXISTS col.non_existing");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a integer)");
        }
    }

    @Test
    public void testDropRowFieldInArray()
    {
        if (!hasBehavior(SUPPORTS_DROP_FIELD_IN_ARRAY)) {
            if (!hasBehavior(SUPPORTS_DROP_COLUMN) || !hasBehavior(SUPPORTS_ROW_TYPE)) {
                return;
            }
            try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_field_in_array_", "AS SELECT CAST(array[row(1, 2)] AS array(row(x integer, y integer))) AS col")) {
                assertQueryFails(
                        "ALTER TABLE " + table.getName() + " DROP COLUMN col.element.x",
                        ".*does not support.*");
            }
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute,
                "test_drop_field_in_array_",
                "AS SELECT CAST(array[row(1, 2, row(10, 20), array[row(30, 40)])] AS array(row(a integer, b integer, c row(x integer, y integer), d array(row(v integer, w integer))))) AS col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(row(a integer, b integer, c row(x integer, y integer), d array(row(v integer, w integer))))");

            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.element.b");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(row(a integer, c row(x integer, y integer), d array(row(v integer, w integer))))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(array[row(1, row(10, 20), array[row(30, 40)])] AS array(row(a integer, c row(x integer, y integer), d array(row(v integer, w integer)))))");

            // Drop a nested field
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.element.c.y");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(row(a integer, c row(x integer), d array(row(v integer, w integer))))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(array[row(1, row(10), array[row(30, 40)])] AS array(row(a integer, c row(x integer), d array(row(v integer, w integer)))))");

            // Drop a nested field in array
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.element.d.element.v");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(row(a integer, c row(x integer), d array(row(w integer))))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(array[row(1, row(10), array[row(40)])] AS array(row(a integer, c row(x integer), d array(row(w integer)))))");

            // Verify failure when trying to drop unique field in nested row type
            assertQueryFails("ALTER TABLE " + table.getName() + " DROP COLUMN col.element.c.x", ".* Cannot drop the only field in a row type");
            assertQueryFails("ALTER TABLE " + table.getName() + " DROP COLUMN col.element.d.element.w", ".* Cannot drop the only field in a row type");

            // Verify failure when trying to drop non-existing fields
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " DROP COLUMN col.element.c.non_existing",
                    "\\Qline 1:1: Cannot resolve field 'non_existing' within row(x integer) type when dropping [element, c, non_existing] in array(row(a integer, c row(x integer), d array(row(w integer))))");

            // Verify failure when trying to drop non-existing fields
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " DROP COLUMN col.element.d.element.non_existing",
                    "\\Qline 1:1: Cannot resolve field 'non_existing' within row(w integer) type when dropping [element, d, element, non_existing] in array(row(a integer, c row(x integer), d array(row(w integer))))");

            // Drop a row having fields
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.element.c");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(row(a integer, d array(row(w integer))))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(array[row(1, array[row(40)])] AS array(row(a integer, d array(row(w integer)))))");

            // Drop a array(row) having fields
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.element.d");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(row(a integer))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(array[row(1)] AS array(row(a integer)))");

            // Denote to array without 'element' designator
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " DROP COLUMN col.a",
                    "\\QARRAY type should be denoted by 'element' in the path; found 'a'");

            // Specify non-existing fields with IF EXISTS option
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN IF EXISTS non_existing.a");
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN IF EXISTS col.element.non_existing");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(row(a integer))");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute,
                "test_drop_field_in_array_nested_",
                "AS SELECT CAST(array[array[row(1, 2, row(10, 20), array[row(30, 40)])]] AS array(array(row(a integer, b integer, c row(x integer, y integer), d array(row(v integer, w integer)))))) AS col")) {

            // Use path ending with element
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " DROP COLUMN col.element.element",
                    "\\Qline 1:1: Field path [element, element] does not point to row field");
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " DROP COLUMN col.element.element.d.element",
                    "\\Qline 1:1: Field path [element, element, d, element] does not point to row field");

            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.element.element.b");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(array(row(a integer, c row(x integer, y integer), d array(row(v integer, w integer)))))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(array[array[row(1, row(10, 20), array[row(30, 40)])]] AS array(array(row(a integer, c row(x integer, y integer), d array(row(v integer, w integer))))))");

            // Drop a nested field
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.element.element.c.y");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(array(row(a integer, c row(x integer), d array(row(v integer, w integer)))))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(array[array[row(1, row(10), array[row(30, 40)])]] AS array(array(row(a integer, c row(x integer), d array(row(v integer, w integer))))))");

            // Drop a nested field in array
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.element.element.d.element.v");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(array(row(a integer, c row(x integer), d array(row(w integer)))))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(array[array[row(1, row(10), array[row(40)])]] AS array(array(row(a integer, c row(x integer), d array(row(w integer))))))");
        }
    }

    @Test
    public void testDropRowFieldWhenDuplicates()
    {
        skipTestUnless(hasBehavior(SUPPORTS_DROP_FIELD));

        try (TestTable table = new TestTable(getQueryRunner()::execute,
                "test_drop_duplicated_field_",
                "AS SELECT CAST(row(1, 2, 3) AS row(a integer, a integer, b integer)) AS col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a integer, a integer, b integer)");

            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " DROP COLUMN col.a",
                    "\\QField path [a] within row(a integer, a integer, b integer) is ambiguous");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a integer, a integer, b integer)");
        }
    }

    @Test
    public void testDropRowFieldCaseSensitivity()
    {
        skipTestUnless(hasBehavior(SUPPORTS_DROP_FIELD));

        try (TestTable table = new TestTable(getQueryRunner()::execute,
                "test_drop_row_field_case_sensitivity_",
                "AS SELECT CAST(row(1, 2) AS row(lower integer, \"UPPER\" integer)) AS col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(lower integer, UPPER integer)");

            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " DROP COLUMN col.LOWER",
                    "\\Qline 1:1: Cannot resolve field 'LOWER' within row(lower integer, UPPER integer) type when dropping [LOWER] in row(lower integer, UPPER integer)");
            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " DROP COLUMN col.upper",
                    "\\Qline 1:1: Cannot resolve field 'upper' within row(lower integer, UPPER integer) type when dropping [upper] in row(lower integer, UPPER integer)");

            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.\"UPPER\"");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(lower integer)");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(row(1) AS row(lower integer))");
        }
    }

    @Test
    public void testDropAmbiguousRowFieldCaseSensitivity()
    {
        skipTestUnless(hasBehavior(SUPPORTS_DROP_FIELD));

        try (TestTable table = new TestTable(getQueryRunner()::execute,
                "test_drop_row_field_case_sensitivity_",
                """
                AS SELECT CAST(row(1, 2, 3, 4, 5) AS
                row("sOME_FIELd" integer, "some_field" integer, "SomE_Field" integer, "SOME_FIELD" integer, "sOME_FieLd" integer)) AS col
                """)) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(sOME_FIELd integer, some_field integer, SomE_Field integer, SOME_FIELD integer, sOME_FieLd integer)");

            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.some_field");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(sOME_FIELd integer, SomE_Field integer, SOME_FIELD integer, sOME_FieLd integer)");

            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.\"SomE_Field\"");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(sOME_FIELd integer, SOME_FIELD integer, sOME_FieLd integer)");

            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN col.\"SOME_FIELD\"");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(sOME_FIELd integer, sOME_FieLd integer)");

            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(row(1, 5) AS row(\"sOME_FIELd\" integer, \"sOME_FieLd\" integer))");
        }
    }

    @Test
    public void testDropAndAddColumnWithSameName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_DROP_COLUMN) && hasBehavior(SUPPORTS_ADD_COLUMN));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_add_column", "AS SELECT 1 x, 2 y, 3 z")) {
            assertUpdate("ALTER TABLE " + table.getName() + " DROP COLUMN y");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 3)");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN y int");
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 3, NULL)");
        }
    }

    @Test
    public void testRenameColumn()
    {
        if (!hasBehavior(SUPPORTS_RENAME_COLUMN)) {
            assertQueryFails("ALTER TABLE nation RENAME COLUMN nationkey TO test_rename_column", "This connector does not support renaming columns");
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String tableName;
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_rename_column_", "AS SELECT 'some value' x")) {
            tableName = table.getName();
            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN x TO before_y");
            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN IF EXISTS before_y TO y");
            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN IF EXISTS columnNotExists TO y");
            assertQuery("SELECT y FROM " + tableName, "VALUES 'some value'");

            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN y TO Z"); // 'Z' is upper-case, not delimited
            assertQuery(
                    "SELECT z FROM " + tableName, // 'z' is lower-case, not delimited
                    "VALUES 'some value'");

            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN IF EXISTS z TO a");
            assertQuery(
                    "SELECT a FROM " + tableName,
                    "VALUES 'some value'");

            // There should be exactly one column
            assertQuery("SELECT * FROM " + tableName, "VALUES 'some value'");
        }

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " RENAME COLUMN columnNotExists TO y");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " RENAME COLUMN IF EXISTS columnNotExists TO y");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    public void testRenameColumnWithComment()
    {
        skipTestUnless(hasBehavior(SUPPORTS_RENAME_COLUMN) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_rename_column_", "(col INT COMMENT 'test column comment')")) {
            assertThat(getColumnComment(table.getName(), "col")).isEqualTo("test column comment");

            assertUpdate("ALTER TABLE " + table.getName() + " RENAME COLUMN col TO renamed_col");
            assertThat(getColumnComment(table.getName(), "renamed_col")).isEqualTo("test column comment");
        }
    }

    @Test
    public void testRenameRowField()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_ROW_TYPE));

        if (!hasBehavior(SUPPORTS_RENAME_FIELD)) {
            try (TestTable table = new TestTable(getQueryRunner()::execute, "test_rename_field_", "AS SELECT CAST(row(1) AS row(x integer)) AS col")) {
                assertQueryFails(
                        "ALTER TABLE " + table.getName() + " RENAME COLUMN col.x TO x_renamed",
                        "This connector does not support renaming fields");
            }
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute,
                "test_add_field_",
                "AS SELECT CAST(row(1, row(10)) AS row(a integer, b row(x integer))) AS col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a integer, b row(x integer))");

            assertUpdate("ALTER TABLE " + table.getName() + " RENAME COLUMN col.a TO a_renamed");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a_renamed integer, b row(x integer))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(row(1, row(10)) AS row(a_renamed integer, b row(x integer)))");

            // Rename a nested field
            assertUpdate("ALTER TABLE " + table.getName() + " RENAME COLUMN col.b.x TO x_renamed");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a_renamed integer, b row(x_renamed integer))");
            assertThat(query("SELECT * FROM " + table.getName())).matches("SELECT CAST(row(1, row(10)) AS row(a_renamed integer, b row(x_renamed integer)))");

            // Specify not existing fields with IF EXISTS option
            assertUpdate("ALTER TABLE " + table.getName() + " RENAME COLUMN IF EXISTS col.a_missing TO a_missing_renamed");
            assertUpdate("ALTER TABLE " + table.getName() + " RENAME COLUMN IF EXISTS col.b.x_missing TO x_missing_renamed");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(a_renamed integer, b row(x_renamed integer))");

            // Specify existing fields without IF EXISTS option
            assertQueryFails("ALTER TABLE " + table.getName() + " RENAME COLUMN col.a_renamed TO a_renamed", ".* Field 'a_renamed' already exists");
        }
    }

    @Test
    public void testRenameRowFieldCaseSensitivity()
    {
        skipTestUnless(hasBehavior(SUPPORTS_RENAME_FIELD));

        try (TestTable table = new TestTable(getQueryRunner()::execute,
                "test_add_row_field_case_sensitivity_",
                "AS SELECT CAST(row(1, 2) AS row(lower integer, \"UPPER\" integer)) AS col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(lower integer, UPPER integer)");

            assertQueryFails("ALTER TABLE " + table.getName() + " RENAME COLUMN col.lower TO UPPER", ".* Field 'upper' already exists");
            assertQueryFails("ALTER TABLE " + table.getName() + " RENAME COLUMN col.lower TO upper", ".* Field 'upper' already exists");

            assertUpdate("ALTER TABLE " + table.getName() + " RENAME COLUMN col.lower TO LOWER_RENAMED");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(lower_renamed integer, UPPER integer)");

            assertUpdate("ALTER TABLE " + table.getName() + " RENAME COLUMN col.\"UPPER\" TO upper_renamed");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(lower_renamed integer, upper_renamed integer)");

            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("SELECT CAST(row(1, 2) AS row(lower_renamed integer, upper_renamed integer))");
        }
    }

    @Test
    public void testSetColumnType()
    {
        if (!hasBehavior(SUPPORTS_SET_COLUMN_TYPE)) {
            assertQueryFails("ALTER TABLE nation ALTER COLUMN nationkey SET DATA TYPE bigint", "This connector does not support setting column types");
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_column_type_", "AS SELECT CAST(123 AS integer) AS col")) {
            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col SET DATA TYPE bigint");

            assertThat(getColumnType(table.getName(), "col")).isEqualTo("bigint");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES bigint '123'");
        }
    }

    @Test
    public void testSetColumnTypes()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_COLUMN_TYPE) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        for (SetColumnTypeSetup setup : setColumnTypesDataProvider()) {
            TestTable table;
            try {
                table = new TestTable(getQueryRunner()::execute, "test_set_column_type_", " AS SELECT CAST(" + setup.sourceValueLiteral + " AS " + setup.sourceColumnType + ") AS col");
            }
            catch (Exception e) {
                verifyUnsupportedTypeException(e, setup.sourceColumnType);
                continue;
            }
            try (table) {
                Runnable setColumnType = () -> assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col SET DATA TYPE " + setup.newColumnType);
                if (setup.unsupportedType) {
                    assertThatThrownBy(setColumnType::run)
                            .satisfies(this::verifySetColumnTypeFailurePermissible);
                    return;
                }
                setColumnType.run();

                assertThat(getColumnType(table.getName(), "col")).isEqualTo(setup.newColumnType);
                assertThat(query("SELECT * FROM " + table.getName()))
                        .skippingTypesCheck()
                        .matches("SELECT " + setup.newValueLiteral);
            }
        }
    }

    private List<SetColumnTypeSetup> setColumnTypesDataProvider()
    {
        return setColumnTypeSetupData().stream()
                .map(this::filterSetColumnTypesDataProvider)
                .flatMap(Optional::stream)
                .collect(toList());
    }

    protected Optional<SetColumnTypeSetup> filterSetColumnTypesDataProvider(SetColumnTypeSetup setup)
    {
        return Optional.of(setup);
    }

    private List<SetColumnTypeSetup> setColumnTypeSetupData()
    {
        return ImmutableList.<SetColumnTypeSetup>builder()
                .add(new SetColumnTypeSetup("tinyint", "TINYINT '127'", "smallint"))
                .add(new SetColumnTypeSetup("tinyint", "TINYINT '126'", "integer"))
                .add(new SetColumnTypeSetup("tinyint", "TINYINT '125'", "bigint"))
                .add(new SetColumnTypeSetup("smallint", "SMALLINT '32767'", "integer"))
                .add(new SetColumnTypeSetup("smallint", "SMALLINT '32766'", "bigint"))
                .add(new SetColumnTypeSetup("integer", "2147483647", "bigint"))
                .add(new SetColumnTypeSetup("bigint", "BIGINT '-2147483648'", "integer"))
                .add(new SetColumnTypeSetup("bigint", "BIGINT '-32768'", "smallint"))
                .add(new SetColumnTypeSetup("bigint", "BIGINT '-128'", "tinyint"))
                .add(new SetColumnTypeSetup("real", "REAL '10.3'", "double"))
                .add(new SetColumnTypeSetup("real", "REAL 'NaN'", "double"))
                .add(new SetColumnTypeSetup("decimal(5,3)", "12.345", "decimal(10,3)")) // short decimal -> short decimal
                .add(new SetColumnTypeSetup("decimal(28,3)", "12.345", "decimal(38,3)")) // long decimal -> long decimal
                .add(new SetColumnTypeSetup("decimal(5,3)", "12.345", "decimal(38,3)")) // short decimal -> long decimal
                .add(new SetColumnTypeSetup("decimal(5,3)", "12.340", "decimal(5,2)"))
                .add(new SetColumnTypeSetup("decimal(5,3)", "12.349", "decimal(5,2)"))
                .add(new SetColumnTypeSetup("time(3)", "TIME '15:03:00.123'", "time(6)"))
                .add(new SetColumnTypeSetup("time(6)", "TIME '15:03:00.123000'", "time(3)"))
                .add(new SetColumnTypeSetup("time(6)", "TIME '15:03:00.123999'", "time(3)"))
                .add(new SetColumnTypeSetup("timestamp(3)", "TIMESTAMP '2020-02-12 15:03:00.123'", "timestamp(6)"))
                .add(new SetColumnTypeSetup("timestamp(6)", "TIMESTAMP '2020-02-12 15:03:00.123000'", "timestamp(3)"))
                .add(new SetColumnTypeSetup("timestamp(6)", "TIMESTAMP '2020-02-12 15:03:00.123999'", "timestamp(3)"))
                .add(new SetColumnTypeSetup("timestamp(3) with time zone", "TIMESTAMP '2020-02-12 15:03:00.123 +01:00'", "timestamp(6) with time zone"))
                .add(new SetColumnTypeSetup("varchar(100)", "'shorten-varchar'", "varchar(50)"))
                .add(new SetColumnTypeSetup("char(25)", "'shorten-char'", "char(20)"))
                .add(new SetColumnTypeSetup("char(20)", "'char-to-varchar'", "varchar"))
                .add(new SetColumnTypeSetup("varchar", "'varchar-to-char'", "char(20)"))
                .add(new SetColumnTypeSetup("array(integer)", "array[1]", "array(bigint)"))
                .add(new SetColumnTypeSetup("row(x integer)", "row(1)", "row(x bigint)"))
                .add(new SetColumnTypeSetup("row(x integer)", "row(1)", "row(y integer)", "cast(row(NULL) as row(x integer))")) // rename a field
                .add(new SetColumnTypeSetup("row(x integer, y integer)", "row(1, 2)", "row(x integer, z integer)", "cast(row(1, NULL) as row(x integer, z integer))")) // rename a field, but not all fields
                .add(new SetColumnTypeSetup("row(x integer)", "row(1)", "row(x integer, y integer)", "cast(row(1, NULL) as row(x integer, y integer))")) // add a new field
                .add(new SetColumnTypeSetup("row(x integer, y integer)", "row(1, 2)", "row(x integer)", "cast(row(1) as row(x integer))")) // remove an existing field
                .add(new SetColumnTypeSetup("row(x integer, y integer)", "row(1, 2)", "row(y integer, x integer)", "cast(row(2, 1) as row(y integer, x integer))")) // reorder fields
                .add(new SetColumnTypeSetup("row(x integer, y integer)", "row(1, 2)", "row(z integer, y integer, x integer)", "cast(row(null, 2, 1) as row(z integer, y integer, x integer))")) // reorder fields with a new field
                .add(new SetColumnTypeSetup("row(x row(nested integer))", "row(row(1))", "row(x row(nested bigint))", "cast(row(row(1)) as row(x row(nested bigint)))")) // update a nested field
                .add(new SetColumnTypeSetup("row(x row(a integer, b integer))", "row(row(1, 2))", "row(x row(b integer, a integer))", "cast(row(row(2, 1)) as row(x row(b integer, a integer)))")) // reorder a nested field
                .build();
    }

    public record SetColumnTypeSetup(String sourceColumnType, String sourceValueLiteral, String newColumnType, String newValueLiteral, boolean unsupportedType)
    {
        public SetColumnTypeSetup(String sourceColumnType, String sourceValueLiteral, String newColumnType)
        {
            this(sourceColumnType, sourceValueLiteral, newColumnType, "CAST(CAST(%s AS %s) AS %s)".formatted(sourceValueLiteral, sourceColumnType, newColumnType));
        }

        public SetColumnTypeSetup(String sourceColumnType, String sourceValueLiteral, String newColumnType, String newValueLiteral)
        {
            this(sourceColumnType, sourceValueLiteral, newColumnType, newValueLiteral, false);
        }

        public SetColumnTypeSetup
        {
            requireNonNull(sourceColumnType, "sourceColumnType is null");
            requireNonNull(sourceValueLiteral, "sourceValueLiteral is null");
            requireNonNull(newColumnType, "newColumnType is null");
            requireNonNull(newValueLiteral, "newValueLiteral is null");
        }

        public SetColumnTypeSetup withNewColumnType(String newColumnType)
        {
            checkState(!unsupportedType);
            return new SetColumnTypeSetup(sourceColumnType, sourceValueLiteral, newColumnType);
        }

        public SetColumnTypeSetup withNewValueLiteral(String newValueLiteral)
        {
            checkState(!unsupportedType);
            return new SetColumnTypeSetup(sourceColumnType, sourceValueLiteral, newColumnType, newValueLiteral, unsupportedType);
        }

        public SetColumnTypeSetup asUnsupported()
        {
            return new SetColumnTypeSetup(sourceColumnType, sourceValueLiteral, newColumnType, newValueLiteral, true);
        }
    }

    @Test
    public void testSetColumnTypeWithNotNull()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_COLUMN_TYPE) && hasBehavior(SUPPORTS_NOT_NULL_CONSTRAINT));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_column_type_null_", "(col int NOT NULL)")) {
            assertThat(columnIsNullable(table.getName(), "col")).isFalse();

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col SET DATA TYPE bigint");
            assertThat(columnIsNullable(table.getName(), "col")).isFalse();
        }
    }

    @Test
    public void testSetColumnTypeWithComment()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_COLUMN_TYPE) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_column_type_comment_", "(col int COMMENT 'test comment')")) {
            assertThat(getColumnComment(table.getName(), "col")).isEqualTo("test comment");

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col SET DATA TYPE bigint");
            assertThat(getColumnComment(table.getName(), "col")).isEqualTo("test comment");
        }
    }

    @Test
    public void testSetColumnTypeWithDefaultColumn()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_COLUMN_TYPE) && hasBehavior(SUPPORTS_INSERT));

        try (TestTable table = createTableWithDefaultColumns()) {
            // col_default column inserts 43 by default
            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col_default SET DATA TYPE bigint");
            assertUpdate("INSERT INTO " + table.getName() + " (col_required, col_required2) VALUES (1, 10)", 1);
            assertQuery("SELECT col_default FROM " + table.getName(), "VALUES 43");
        }
    }

    @Test
    public void testSetColumnIncompatibleType()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_COLUMN_TYPE) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_invalid_column_type_", "AS SELECT 'test' AS col")) {
            assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col SET DATA TYPE integer"))
                    .satisfies(this::verifySetColumnTypeFailurePermissible);
        }
    }

    @Test
    public void testSetColumnOutOfRangeType()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_COLUMN_TYPE) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_column_type_invalid_range_", "AS SELECT CAST(9223372036854775807 AS bigint) AS col")) {
            assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col SET DATA TYPE integer"))
                    .satisfies(this::verifySetColumnTypeFailurePermissible);
        }
    }

    protected void verifySetColumnTypeFailurePermissible(Throwable e)
    {
        throw new AssertionError("Unexpected set column type failure", e);
    }

    @Test
    public void testSetFieldType()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_ROW_TYPE));

        if (!hasBehavior(SUPPORTS_SET_FIELD_TYPE)) {
            try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_field_type_", "(col row(field int))")) {
                assertQueryFails(
                        "ALTER TABLE " + table.getName() + " ALTER COLUMN col.field SET DATA TYPE bigint",
                        "This connector does not support setting field types");
            }
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_field_type_", "AS SELECT CAST(row(123) AS row(field integer)) AS col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(field integer)");

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col.field SET DATA TYPE bigint");

            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(field bigint)");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("SELECT row(bigint '123')");
        }
    }

    @Test
    public void testSetFieldTypes()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_FIELD_TYPE) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        for (SetColumnTypeSetup setup : setFieldTypesDataProvider()) {
            TestTable table;
            try {
                table = new TestTable(
                        getQueryRunner()::execute,
                        "test_set_field_type_",
                        " AS SELECT CAST(row(" + setup.sourceValueLiteral + ") AS row(field " + setup.sourceColumnType + ")) AS col");
            }
            catch (Exception e) {
                verifyUnsupportedTypeException(e, setup.sourceColumnType);
                continue;
            }
            try (table) {
                Runnable setFieldType = () -> assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col.field SET DATA TYPE " + setup.newColumnType);
                if (setup.unsupportedType) {
                    assertThatThrownBy(setFieldType::run)
                            .satisfies(this::verifySetFieldTypeFailurePermissible);
                    return;
                }
                setFieldType.run();

                assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(field " + setup.newColumnType + ")");
                assertThat(query("SELECT * FROM " + table.getName()))
                        .skippingTypesCheck()
                        .matches("SELECT row(" + setup.newValueLiteral + ")");
            }
        }
    }

    public List<SetColumnTypeSetup> setFieldTypesDataProvider()
    {
        return setColumnTypeSetupData().stream()
                .map(this::filterSetFieldTypesDataProvider)
                .flatMap(Optional::stream)
                .collect(toList());
    }

    protected Optional<SetColumnTypeSetup> filterSetFieldTypesDataProvider(SetColumnTypeSetup setup)
    {
        return Optional.of(setup);
    }

    @Test
    public void testSetFieldTypeCaseSensitivity()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_FIELD_TYPE) && hasBehavior(SUPPORTS_NOT_NULL_CONSTRAINT));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_field_type_case_", " AS SELECT CAST(row(1) AS row(\"UPPER\" integer)) col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(UPPER integer)");

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col.upper SET DATA TYPE bigint");
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("row(UPPER bigint)");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("SELECT CAST(row(1) AS row(UPPER bigint))");
        }
    }

    @Test
    public void testSetFieldTypeWithNotNull()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_FIELD_TYPE) && hasBehavior(SUPPORTS_NOT_NULL_CONSTRAINT));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_field_type_null_", "(col row(field int) NOT NULL)")) {
            assertThat(columnIsNullable(table.getName(), "col")).isFalse();

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col.field SET DATA TYPE bigint");
            assertThat(columnIsNullable(table.getName(), "col")).isFalse();
        }
    }

    @Test
    public void testSetFieldTypeWithComment()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_FIELD_TYPE) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_field_type_comment_", "(col row(field int) COMMENT 'test comment')")) {
            assertThat(getColumnComment(table.getName(), "col")).isEqualTo("test comment");

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col.field SET DATA TYPE bigint");
            assertThat(getColumnComment(table.getName(), "col")).isEqualTo("test comment");
        }
    }

    @Test
    public void testSetFieldIncompatibleType()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_FIELD_TYPE) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_set_invalid_field_type_",
                "(row_col row(field varchar), nested_col row(field row(nested int)))")) {
            assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN row_col.field SET DATA TYPE row(nested integer)"))
                    .satisfies(this::verifySetFieldTypeFailurePermissible);
            assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN row_col.field SET DATA TYPE integer"))
                    .satisfies(this::verifySetFieldTypeFailurePermissible);
            assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN nested_col.field SET DATA TYPE integer"))
                    .satisfies(this::verifySetFieldTypeFailurePermissible);
        }
    }

    @Test
    public void testSetFieldOutOfRangeType()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_FIELD_TYPE) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_set_field_type_invalid_range_",
                "AS SELECT CAST(row(9223372036854775807) AS row(field bigint)) AS col")) {
            assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col.field SET DATA TYPE integer"))
                    .satisfies(this::verifySetFieldTypeFailurePermissible);
        }
    }

    @Test
    public void testSetFieldTypeInArray()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_ARRAY) && hasBehavior(SUPPORTS_ROW_TYPE));

        if (!hasBehavior(SUPPORTS_SET_FIELD_TYPE_IN_ARRAY)) {
            try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_field_type_in_array_", "(col array(row(field int)))")) {
                assertQueryFails(
                        "ALTER TABLE " + table.getName() + " ALTER COLUMN col.element.field SET DATA TYPE bigint",
                        ".*does not support.*");
            }
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_field_type_in_array_", "AS SELECT CAST(array[row(123)] AS array(row(field integer))) AS col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(row(field integer))");

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col.element.field SET DATA TYPE bigint");

            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(row(field bigint))");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("SELECT array[row(bigint '123')]");
        }
    }

    @Test
    public void testSetFieldTypeInNestedArray()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_FIELD_TYPE_IN_ARRAY) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_ARRAY) && hasBehavior(SUPPORTS_ROW_TYPE));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_field_type_in_nested_array_", "AS SELECT CAST(array[array[row(array[row(123)])]] AS array(array(row(field array(row(a integer)))))) AS col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(array(row(field array(row(a integer)))))");

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col.element.element.field.element.a SET DATA TYPE bigint");

            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(array(row(field array(row(a bigint)))))");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .skippingTypesCheck()
                    .matches("SELECT array[array[row(array[row(bigint '123')])]]");

            assertQueryFails(
                    "ALTER TABLE " + table.getName() + " ALTER COLUMN col.element.element SET DATA TYPE bigint",
                    "\\Qline 1:1: Field path [col, element, element] does not point to row field");
        }
    }

    @Test
    public void testSetFieldMapKeyType()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_MAP_TYPE) && hasBehavior(SUPPORTS_ROW_TYPE));

        String tableDefinition = "AS SELECT CAST(map(array[row(1)], array[2]) AS map(row(field integer), integer)) AS col";
        if (!hasBehavior(SUPPORTS_SET_FIELD_TYPE_IN_MAP)) {
            try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_field_type_in_map", tableDefinition)) {
                assertQueryFails("ALTER TABLE " + table.getName() + " ALTER COLUMN col.key.field SET DATA TYPE bigint", ".*does not support.*");
            }
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_field_type_in_map", tableDefinition)) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("map(row(field integer), integer)");

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col.key.field SET DATA TYPE bigint");

            assertThat(getColumnType(table.getName(), "col")).isEqualTo("map(row(field bigint), integer)");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("SELECT CAST(map(array[row(1)], array[2]) AS map(row(field bigint), integer))");
        }
    }

    @Test
    public void testSetFieldMapValueType()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_MAP_TYPE) && hasBehavior(SUPPORTS_ROW_TYPE));

        String tableDefinition = "AS SELECT CAST(map(array[1], array[row(2)]) AS map(integer, row(field integer))) AS col";
        if (!hasBehavior(SUPPORTS_SET_FIELD_TYPE_IN_MAP)) {
            try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_field_type_in_map", tableDefinition)) {
                assertQueryFails("ALTER TABLE " + table.getName() + " ALTER COLUMN col.value.field SET DATA TYPE bigint", ".*does not support.*");
            }
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_set_field_type_in_map", tableDefinition)) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("map(integer, row(field integer))");

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col.value.field SET DATA TYPE bigint");

            assertThat(getColumnType(table.getName(), "col")).isEqualTo("map(integer, row(field bigint))");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("SELECT CAST(map(array[1], array[row(2)]) AS map(integer, row(field bigint)))");
        }
    }

    @Test
    public void testSetNestedFieldMapKeyType()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_FIELD_TYPE_IN_ARRAY) && hasBehavior(SUPPORTS_SET_FIELD_TYPE_IN_MAP) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_ARRAY) && hasBehavior(SUPPORTS_MAP_TYPE) && hasBehavior(SUPPORTS_ROW_TYPE));

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_set_nested_field_type_in_map",
                "AS SELECT CAST(array[map(array[row(1)], array[2])] AS array(map(row(field integer), integer))) AS col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(map(row(field integer), integer))");

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col.element.key.field SET DATA TYPE bigint");

            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(map(row(field bigint), integer))");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("SELECT CAST(array[map(array[row(1)], array[2])] AS array(map(row(field bigint), integer)))");
        }
    }

    @Test
    public void testSetNestedFieldMapValueType()
    {
        skipTestUnless(hasBehavior(SUPPORTS_SET_FIELD_TYPE_IN_ARRAY) && hasBehavior(SUPPORTS_SET_FIELD_TYPE_IN_MAP) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_ARRAY) && hasBehavior(SUPPORTS_MAP_TYPE) && hasBehavior(SUPPORTS_ROW_TYPE));

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_set_nested_field_type_in_map",
                "AS SELECT CAST(array[map(array[1], array[row(2)])] AS array(map(integer, row(field integer)))) AS col")) {
            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(map(integer, row(field integer)))");

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col.element.value.field SET DATA TYPE bigint");

            assertThat(getColumnType(table.getName(), "col")).isEqualTo("array(map(integer, row(field bigint)))");
            assertThat(query("SELECT * FROM " + table.getName()))
                    .matches("SELECT CAST(array[map(array[1], array[row(2)])] AS array(map(integer, row(field bigint))))");
        }
    }

    protected void verifySetFieldTypeFailurePermissible(Throwable e)
    {
        throw new AssertionError("Unexpected set field type failure", e);
    }

    protected String getColumnType(String tableName, String columnName)
    {
        return (String) computeScalar(format("SELECT data_type FROM information_schema.columns WHERE table_schema = CURRENT_SCHEMA AND table_name = '%s' AND column_name = '%s'",
                tableName,
                columnName));
    }

    @Test
    public void testDropNotNullConstraint()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_NOT_NULL_CONSTRAINT));

        if (!hasBehavior(SUPPORTS_DROP_NOT_NULL_CONSTRAINT)) {
            try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_not_null_", "(col integer NOT NULL)")) {
                assertQueryFails(
                        "ALTER TABLE " + table.getName() + " ALTER COLUMN col DROP NOT NULL",
                        "This connector does not support dropping a not null constraint");
            }
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_not_null_", "(col integer NOT NULL)")) {
            assertThat(columnIsNullable(table.getName(), "col")).isFalse();

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col DROP NOT NULL");
            assertThat(columnIsNullable(table.getName(), "col")).isTrue();

            assertUpdate("INSERT INTO " + table.getName() + " VALUES NULL", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES NULL");
        }
    }

    @Test
    public void testDropNotNullConstraintWithColumnComment()
    {
        skipTestUnless(hasBehavior(SUPPORTS_DROP_NOT_NULL_CONSTRAINT) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT));

        // Verify DROP NOT NULL preserves the existing column comment
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_not_null_", "(col integer NOT NULL COMMENT 'test comment')")) {
            assertThat(getColumnComment(table.getName(), "col")).isEqualTo("test comment");
            assertThat(columnIsNullable(table.getName(), "col")).isFalse();

            assertUpdate("ALTER TABLE " + table.getName() + " ALTER COLUMN col DROP NOT NULL");
            assertThat(columnIsNullable(table.getName(), "col")).isTrue();

            assertThat(getColumnComment(table.getName(), "col")).isEqualTo("test comment");
        }
    }

    @Test
    public void testCreateTable()
    {
        String tableName = "test_create_" + randomNameSuffix();
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            assertQueryFails("CREATE TABLE " + tableName + " (a bigint, b double, c varchar(50))", "This connector does not support creating tables");
            return;
        }

        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()) // prime the cache, if any
                .doesNotContain(tableName);
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double, c varchar(50))");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .contains(tableName);
        assertTableColumnNames(tableName, "a", "b", "c");
        assertThat(getTableComment(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), tableName)).isNull();

        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet())
                .doesNotContain(tableName);

        assertQueryFails("CREATE TABLE " + tableName + " (a bad_type)", ".* Unknown type 'bad_type' for column 'a'");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();

        // TODO (https://github.com/trinodb/trino/issues/5901) revert to longer name when Oracle version is updated
        tableName = "test_cr_not_exists_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b varchar(50), c double)");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (d bigint, e varchar(50))");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();

        // Test CREATE TABLE LIKE
        tableName = "test_create_orig_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double, c varchar(50))");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertTableColumnNames(tableName, "a", "b", "c");

        String tableNameLike = "test_create_like_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableNameLike + " (LIKE " + tableName + ", d bigint, e varchar(50))");
        assertThat(getQueryRunner().tableExists(getSession(), tableNameLike)).isTrue();
        assertTableColumnNames(tableNameLike, "a", "b", "c", "d", "e");

        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();

        assertUpdate("DROP TABLE " + tableNameLike);
        assertThat(getQueryRunner().tableExists(getSession(), tableNameLike)).isFalse();
    }

    @Test
    public void testCreateSchemaWithNonLowercaseOwnerName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_SCHEMA));

        Session newSession = Session.builder(getSession())
                .setIdentity(Identity.ofUser("ADMIN"))
                .build();
        String schemaName = "test_schema_create_uppercase_owner_name_" + randomNameSuffix();
        assertUpdate(newSession, createSchemaSql(schemaName));
        assertThat(query(newSession, "SHOW SCHEMAS"))
                .skippingTypesCheck()
                .containsAll(format("VALUES '%s'", schemaName));
        assertUpdate(newSession, "DROP SCHEMA " + schemaName);
    }

    @Test
    public void testCreateOrReplaceTableWhenTableDoesNotExist()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        String table = "test_create_or_replace_" + randomNameSuffix();
        if (!hasBehavior(SUPPORTS_CREATE_OR_REPLACE_TABLE)) {
            assertQueryFails("CREATE OR REPLACE TABLE " + table + " (a bigint, b double, c varchar(50))", "This connector does not support replacing tables");
            return;
        }

        try {
            assertUpdate("CREATE OR REPLACE TABLE " + table + " (a bigint, b double, c varchar(50))");
            assertQueryReturnsEmptyResult("SELECT * FROM " + table);
        } finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testCreateOrReplaceTableAsSelectWhenTableDoesNotExists()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        String table = "test_create_or_replace_" + randomNameSuffix();
        @Language("SQL") String query = "SELECT nationkey, name, regionkey FROM nation";
        @Language("SQL") String rowCountQuery = "SELECT count(*) FROM nation";
        if (!hasBehavior(SUPPORTS_CREATE_OR_REPLACE_TABLE)) {
            assertQueryFails("CREATE OR REPLACE TABLE " + table + " AS " + query, "This connector does not support replacing tables");
            return;
        }

        try {
            assertUpdate("CREATE OR REPLACE TABLE " + table + " AS " + query, rowCountQuery);
            assertQuery("SELECT * FROM " + table, query);
        } finally {
            assertUpdate("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    public void testCreateOrReplaceTableWhenTableAlreadyExistsSameSchema()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        if (!hasBehavior(SUPPORTS_CREATE_OR_REPLACE_TABLE)) {
            // covered in testCreateOrReplaceTableWhenTableDoesNotExist
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_or_replace_", "AS SELECT CAST(1 AS BIGINT) AS nationkey, 'test' AS name, CAST(2 AS BIGINT) AS regionkey FROM nation LIMIT 1")) {
            @Language("SQL") String query = "SELECT nationkey, name, regionkey FROM nation";
            @Language("SQL") String rowCountQuery = "SELECT count(*) FROM nation";
            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " AS " + query, rowCountQuery);
            assertQuery("SELECT * FROM " + table.getName(), query);
        }
    }

    @Test
    public void testCreateOrReplaceTableWhenTableAlreadyExistsSameSchemaNoData()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        if (!hasBehavior(SUPPORTS_CREATE_OR_REPLACE_TABLE)) {
            // covered in testCreateOrReplaceTableWhenTableDoesNotExist
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_or_replace_", " AS SELECT nationkey, name, regionkey FROM nation")) {
            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " AS SELECT nationkey, name, regionkey FROM nation WITH NO DATA", 0L);
            assertQueryReturnsEmptyResult("SELECT * FROM " + table.getName());
        }
    }

    @Test
    public void testCreateOrReplaceTableWithNewColumnNames()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        if (!hasBehavior(SUPPORTS_CREATE_OR_REPLACE_TABLE)) {
            // covered in testCreateOrReplaceTableWhenTableDoesNotExist
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_or_replace_", " AS SELECT nationkey, name, regionkey FROM nation")) {
            assertTableColumnNames(table.getName(), "nationkey", "name", "regionkey");
            @Language("SQL") String query = "SELECT nationkey AS nationkey_new, name AS name_new_2, regionkey AS region_key_new FROM nation";
            @Language("SQL") String rowCountQuery = "SELECT count(*) FROM nation";
            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " AS " + query, rowCountQuery);
            assertTableColumnNames(table.getName(), "nationkey_new", "name_new_2", "region_key_new");
            assertQuery("SELECT * FROM " + table.getName(), query);
        }
    }

    @Test
    public void testCreateOrReplaceTableWithDifferentDataType()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        if (!hasBehavior(SUPPORTS_CREATE_OR_REPLACE_TABLE)) {
            // covered in testCreateOrReplaceTableWhenTableDoesNotExist
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_or_replace_", " AS SELECT nationkey, name FROM nation")) {
            @Language("SQL") String query = "SELECT name AS nationkey, nationkey AS name FROM nation";
            @Language("SQL") String rowCountQuery = "SELECT count(*) FROM nation";
            assertUpdate("CREATE OR REPLACE TABLE " + table.getName() + " AS " + query, rowCountQuery);
            assertQuery(getSession(), "SELECT * FROM " + table.getName(), query);
        }
    }

    @Test
    public void testCreateSchemaWithLongName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_SCHEMA));

        String baseSchemaName = "test_create_" + randomNameSuffix();

        int maxLength = maxSchemaNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validSchemaName = baseSchemaName + "z".repeat(maxLength - baseSchemaName.length());
        assertUpdate(createSchemaSql(validSchemaName));
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(validSchemaName);
        assertUpdate("DROP SCHEMA " + validSchemaName);

        if (maxSchemaNameLength().isEmpty()) {
            return;
        }

        String invalidSchemaName = validSchemaName + "z";
        assertThatThrownBy(() -> assertUpdate(createSchemaSql(invalidSchemaName)))
                .satisfies(this::verifySchemaNameLengthFailurePermissible);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(invalidSchemaName);
    }

    @Test
    public void testRenameSchemaToLongName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_RENAME_SCHEMA));

        String sourceSchemaName = "test_rename_source_" + randomNameSuffix();
        assertUpdate(createSchemaSql(sourceSchemaName));

        String baseSchemaName = "test_rename_target_" + randomNameSuffix();

        int maxLength = maxSchemaNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTargetSchemaName = baseSchemaName + "z".repeat(maxLength - baseSchemaName.length());
        assertUpdate("ALTER SCHEMA " + sourceSchemaName + " RENAME TO " + validTargetSchemaName);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(validTargetSchemaName);
        assertUpdate("DROP SCHEMA " + validTargetSchemaName);

        if (maxSchemaNameLength().isEmpty()) {
            return;
        }

        assertUpdate(createSchemaSql(sourceSchemaName));
        String invalidTargetSchemaName = validTargetSchemaName + "z";
        assertThatThrownBy(() -> assertUpdate("ALTER SCHEMA " + sourceSchemaName + " RENAME TO " + invalidTargetSchemaName))
                .satisfies(this::verifySchemaNameLengthFailurePermissible);
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(invalidTargetSchemaName);
    }

    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.empty();
    }

    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        throw new AssertionError("Unexpected schema name length failure", e);
    }

    @Test
    public void testCreateTableWithLongTableName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String baseTableName = "test_create_" + randomNameSuffix();

        int maxLength = maxTableNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTableName = baseTableName + "z".repeat(maxLength - baseTableName.length());
        assertUpdate("CREATE TABLE " + validTableName + " (a bigint)");
        assertThat(getQueryRunner().tableExists(getSession(), validTableName)).isTrue();
        assertUpdate("DROP TABLE " + validTableName);

        if (maxTableNameLength().isEmpty()) {
            return;
        }

        String invalidTableName = validTableName + "z";
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + invalidTableName + " (a bigint)"))
                .satisfies(this::verifyTableNameLengthFailurePermissible);
        assertThat(getQueryRunner().tableExists(getSession(), validTableName)).isFalse();
    }

    @Test
    public void testRenameTableToLongTableName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_RENAME_TABLE));

        String sourceTableName = "test_rename_source_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceTableName + " AS SELECT 123 x", 1);

        String baseTableName = "test_rename_target_" + randomNameSuffix();

        int maxLength = maxTableRenameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTargetTableName = baseTableName + "z".repeat(maxLength - baseTableName.length());
        assertUpdate("ALTER TABLE " + sourceTableName + " RENAME TO " + validTargetTableName);
        assertThat(getQueryRunner().tableExists(getSession(), validTargetTableName)).isTrue();
        assertQuery("SELECT x FROM " + validTargetTableName, "VALUES 123");
        assertUpdate("DROP TABLE " + validTargetTableName);

        if (maxTableRenameLength().isEmpty()) {
            return;
        }

        assertUpdate("CREATE TABLE " + sourceTableName + " AS SELECT 123 x", 1);
        String invalidTargetTableName = validTargetTableName + "z";
        assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + sourceTableName + " RENAME TO " + invalidTargetTableName))
                .satisfies(this::verifyTableNameLengthFailurePermissible);
        assertThat(getQueryRunner().tableExists(getSession(), invalidTargetTableName)).isFalse();
        assertUpdate("DROP TABLE " + sourceTableName);
    }

    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.empty();
    }

    protected OptionalInt maxTableRenameLength()
    {
        return maxTableNameLength();
    }

    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        throw new AssertionError("Unexpected table name length failure", e);
    }

    @Test
    public void testCreateTableWithLongColumnName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String tableName = "test_long_column" + randomNameSuffix();
        String baseColumnName = "col";

        int maxLength = maxColumnNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validColumnName = baseColumnName + "z".repeat(maxLength - baseColumnName.length());
        assertUpdate("CREATE TABLE " + tableName + " (" + validColumnName + " bigint)");
        assertThat(columnExists(tableName, validColumnName)).isTrue();
        assertUpdate("DROP TABLE " + tableName);

        if (maxColumnNameLength().isEmpty()) {
            return;
        }

        String invalidColumnName = validColumnName + "z";
        assertThatThrownBy(() -> assertUpdate("CREATE TABLE " + tableName + " (" + invalidColumnName + " bigint)"))
                .satisfies(this::verifyColumnNameLengthFailurePermissible);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    // TODO: Add test for CREATE TABLE AS SELECT with long column name

    @Test
    public void testAlterTableAddLongColumnName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_ADD_COLUMN));

        String tableName = "test_long_column" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String baseColumnName = "col";
        int maxLength = maxColumnNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTargetColumnName = baseColumnName + "z".repeat(maxLength - baseColumnName.length());
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN " + validTargetColumnName + " int");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();
        assertQuery("SELECT x FROM " + tableName, "VALUES 123");
        assertUpdate("DROP TABLE " + tableName);

        if (maxColumnNameLength().isEmpty()) {
            return;
        }

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);
        String invalidTargetColumnName = validTargetColumnName + "z";
        assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN " + invalidTargetColumnName + " int"))
                .satisfies(this::verifyColumnNameLengthFailurePermissible);
        assertQuery("SELECT x FROM " + tableName, "VALUES 123");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAlterTableRenameColumnToLongName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_RENAME_COLUMN));

        String tableName = "test_long_column" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String baseColumnName = "col";
        int maxLength = maxColumnNameLength()
                // Assume 2^16 is enough for most use cases. Add a bit more to ensure 2^16 isn't actual limit.
                .orElse(65536 + 5);

        String validTargetColumnName = baseColumnName + "z".repeat(maxLength - baseColumnName.length());
        assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN x TO " + validTargetColumnName);
        assertUpdate("INSERT INTO " + tableName + " VALUES 456", 1);
        assertQuery("SELECT " + validTargetColumnName + " FROM " + tableName, "VALUES 123, 456");
        assertThat(query("SHOW STATS FOR " + tableName)).succeeds();
        assertUpdate("DROP TABLE " + tableName);

        if (maxColumnNameLength().isEmpty()) {
            return;
        }

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);
        String invalidTargetTableName = validTargetColumnName + "z";
        assertThatThrownBy(() -> assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN x TO " + invalidTargetTableName))
                .satisfies(this::verifyColumnNameLengthFailurePermissible);
        assertQuery("SELECT x FROM " + tableName, "VALUES 123");

        assertUpdate("DROP TABLE " + tableName);
    }

    protected boolean columnExists(String tableName, String columnName)
    {
        MaterializedResult materializedResult = computeActual(format(
                "SELECT 1 FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'",
                getSession().getSchema().orElseThrow(),
                tableName,
                columnName));
        return materializedResult.getRowCount() == 1;
    }

    protected OptionalInt maxColumnNameLength()
    {
        return OptionalInt.empty();
    }

    protected void verifyColumnNameLengthFailurePermissible(Throwable e)
    {
        throw new AssertionError("Unexpected column name length failure", e);
    }

    @Test
    public void testCreateTableWithTableComment()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String tableName = "test_create_" + randomNameSuffix();

        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT)) {
            assertQueryFails("CREATE TABLE " + tableName + " (a bigint) COMMENT 'test comment'", "This connector does not support creating tables with table comment");
            return;
        }

        assertUpdate("CREATE TABLE " + tableName + " (a bigint) COMMENT 'test comment'");
        assertThat(getTableComment(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), tableName)).isEqualTo("test comment");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableWithColumnComment()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String tableName = "test_create_" + randomNameSuffix();

        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT)) {
            assertQueryFails("CREATE TABLE " + tableName + " (a bigint COMMENT 'test comment')", "This connector does not support creating tables with column comment");
            return;
        }

        assertUpdate("CREATE TABLE " + tableName + " (a bigint COMMENT 'test comment')");
        assertThat(getColumnComment(tableName, "a")).isEqualTo("test comment");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableSchemaNotFound()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String schemaName = "test_schema_" + randomNameSuffix();
        String tableName = "test_create_no_schema_" + randomNameSuffix();
        try {
            assertQueryFails(
                    format("CREATE TABLE %s.%s (a bigint)", schemaName, tableName),
                    format("Schema %s not found", schemaName));
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s.%s", schemaName, tableName));
        }
    }

    @Test
    public void testCreateTableAsSelect()
    {
        String tableName = "test_ctas" + randomNameSuffix();
        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA)) {
            assertQueryFails("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name, regionkey FROM nation", "This connector does not support creating tables with data");
            return;
        }
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertTableColumnNames(tableName, "name", "regionkey");
        assertThat(getTableComment(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), tableName)).isNull();
        assertUpdate("DROP TABLE " + tableName);

        // Some connectors support CREATE TABLE AS but not the ordinary CREATE TABLE. Let's test CTAS IF NOT EXISTS with a table that is guaranteed to exist.
        assertUpdate("CREATE TABLE IF NOT EXISTS nation AS SELECT nationkey, regionkey FROM nation", 0);
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
                "SELECT * FROM nation WITH DATA",
                "SELECT * FROM nation",
                "SELECT count(*) FROM nation");

        assertCreateTableAsSelect(
                "SELECT * FROM nation WITH NO DATA",
                "SELECT * FROM nation LIMIT 0",
                "SELECT 0");

        // Tests for CREATE TABLE with UNION ALL: exercises PushTableWriteThroughUnion optimizer

        assertCreateTableAsSelect(
                "SELECT name, nationkey, regionkey FROM nation WHERE nationkey % 2 = 0 UNION ALL " +
                        "SELECT name, nationkey, regionkey FROM nation WHERE nationkey % 2 = 1",
                "SELECT name, nationkey, regionkey FROM nation",
                "SELECT count(*) FROM nation");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "true").build(),
                "SELECT CAST(nationkey AS BIGINT) nationkey, regionkey FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT nationkey, regionkey FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT count(*) + 1 FROM nation");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "SELECT CAST(nationkey AS BIGINT) nationkey, regionkey FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT nationkey, regionkey FROM nation UNION ALL " +
                        "SELECT 1234567890, 123",
                "SELECT count(*) + 1 FROM nation");

        // TODO: BigQuery throws table not found at BigQueryClient.insert if we reuse the same table name
        tableName = "test_ctas" + randomNameSuffix();
        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE " + tableName + " AS SELECT name FROM nation");
        assertQuery("SELECT * from " + tableName, "SELECT name FROM nation");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableAsSelectWithTableComment()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        String tableName = "test_ctas_" + randomNameSuffix();

        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT)) {
            assertQueryFails("CREATE TABLE " + tableName + " COMMENT 'test comment' AS SELECT name FROM nation", "This connector does not support creating tables with table comment");
            return;
        }

        assertUpdate("CREATE TABLE " + tableName + " COMMENT 'test comment' AS SELECT name FROM nation", 25);
        assertThat(getTableComment(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), tableName)).isEqualTo("test comment");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableAsSelectSchemaNotFound()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        String schemaName = "test_schema_" + randomNameSuffix();
        String tableName = "test_ctas_no_schema_" + randomNameSuffix();
        try {
            assertQueryFails(
                    format("CREATE TABLE %s.%s AS SELECT name FROM nation", schemaName, tableName),
                    format("Schema %s not found", schemaName));
        }
        finally {
            assertUpdate(format("DROP TABLE IF EXISTS %s.%s", schemaName, tableName));
        }
    }

    @Test
    public void testCreateTableAsSelectWithUnicode()
    {
        // Covered by testCreateTableAsSelect
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        assertCreateTableAsSelect(
                "SELECT '\u2603' unicode",
                "SELECT 1");
    }

    protected void assertCreateTableAsSelect(@Language("SQL") String query, @Language("SQL") String rowCountQuery)
    {
        assertCreateTableAsSelect(getSession(), query, query, rowCountQuery);
    }

    protected void assertCreateTableAsSelect(@Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
    {
        assertCreateTableAsSelect(getSession(), query, expectedQuery, rowCountQuery);
    }

    protected void assertCreateTableAsSelect(Session session, @Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
    {
        String table = "test_ctas_" + randomNameSuffix();
        assertUpdate(session, "CREATE TABLE " + table + " AS " + query, rowCountQuery);
        assertQuery(session, "SELECT * FROM " + table, expectedQuery);
        assertUpdate(session, "DROP TABLE " + table);

        assertThat(getQueryRunner().tableExists(session, table)).isFalse();
    }

    @Test
    public void testCreateTableAsSelectNegativeDate()
    {
        // Covered by testCreateTableAsSelect
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        String tableName = "negative_date_" + randomNameSuffix();

        if (!hasBehavior(SUPPORTS_NEGATIVE_DATE)) {
            assertQueryFails(format("CREATE TABLE %s AS SELECT DATE '-0001-01-01' AS dt", tableName), errorMessageForCreateTableAsSelectNegativeDate("-0001-01-01"));
            return;
        }

        try {
            assertUpdate(format("CREATE TABLE %s AS SELECT DATE '-0001-01-01' AS dt", tableName), 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES DATE '-0001-01-01'");
            assertQuery(format("SELECT * FROM %s WHERE dt = DATE '-0001-01-01'", tableName), "VALUES DATE '-0001-01-01'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Language("RegExp")
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        throw new UnsupportedOperationException("This method should be overridden");
    }

    @Test
    public void testRenameTable()
            throws Exception
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        String tableName = "test_rename_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String renamedTable = "test_rename_new_" + randomNameSuffix();
        if (!hasBehavior(SUPPORTS_RENAME_TABLE)) {
            assertQueryFails("ALTER TABLE " + tableName + " RENAME TO " + renamedTable, "This connector does not support renaming tables");
            assertUpdate("DROP TABLE " + tableName);
            return;
        }

        try {
            assertUpdate("ALTER TABLE " + tableName + " RENAME TO " + renamedTable);
        }
        catch (Throwable e) {
            try (AutoCloseable ignore = () -> assertUpdate("DROP TABLE " + tableName)) {
                throw e;
            }
        }
        assertQuery("SELECT x FROM " + renamedTable, "VALUES 123");

        String testExistsTableName = "test_rename_exists_" + randomNameSuffix();
        assertUpdate("ALTER TABLE IF EXISTS " + renamedTable + " RENAME TO " + testExistsTableName);
        assertQuery("SELECT x FROM " + testExistsTableName, "VALUES 123");

        String uppercaseName = "TEST_RENAME_" + randomNameSuffix(); // Test an upper-case, not delimited identifier
        assertUpdate("ALTER TABLE " + testExistsTableName + " RENAME TO " + uppercaseName);
        assertQuery(
                "SELECT x FROM " + uppercaseName.toLowerCase(ENGLISH), // Ensure select allows for lower-case, not delimited identifier
                "VALUES 123");

        assertUpdate("DROP TABLE " + uppercaseName);

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(getQueryRunner().tableExists(getSession(), renamedTable)).isFalse();

        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " RENAME TO " + renamedTable);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(getQueryRunner().tableExists(getSession(), renamedTable)).isFalse();
    }

    @Test
    public void testRenameTableAcrossSchema()
            throws Exception
    {
        if (!hasBehavior(SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS)) {
            if (!hasBehavior(SUPPORTS_RENAME_TABLE)) {
                abort("Skipping since rename table is not supported at all");
            }
            assertQueryFails("ALTER TABLE nation RENAME TO other_schema.yyyy", "This connector does not support renaming tables across schemas");
            return;
        }

        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            throw new AssertionError("Cannot test ALTER TABLE RENAME across schemas without CREATE SCHEMA, the test needs to be implemented in a connector-specific way");
        }

        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new AssertionError("Cannot test ALTER TABLE RENAME across schemas without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }

        String tableName = "test_rename_old_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String schemaName = "test_schema_" + randomNameSuffix();
        assertUpdate(createSchemaSql(schemaName));

        String renamedTable = "test_rename_new_" + randomNameSuffix();
        try {
            assertUpdate("ALTER TABLE " + tableName + " RENAME TO " + schemaName + "." + renamedTable);
        }
        catch (Throwable e) {
            try (AutoCloseable ignore = () -> assertUpdate("DROP TABLE " + tableName)) {
                throw e;
            }
        }

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertQuery("SELECT x FROM " + schemaName + "." + renamedTable, "VALUES 123");

        assertUpdate("DROP TABLE " + schemaName + "." + renamedTable);
        assertUpdate("DROP SCHEMA " + schemaName);

        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
        assertThat(getQueryRunner().tableExists(Session.builder(getSession()).setSchema(schemaName).build(), renamedTable)).isFalse();
    }

    @Test
    public void testRenameTableToUnqualifiedPreservesSchema()
            throws Exception
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_SCHEMA) && hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_RENAME_TABLE));

        String sourceSchemaName = "test_source_schema_" + randomNameSuffix();
        assertUpdate(createSchemaSql(sourceSchemaName));

        String tableName = "test_rename_unqualified_name_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + sourceSchemaName + "." + tableName + " AS SELECT 123 x", 1);

        String renamedTable = "test_rename_unqualified_name_new_" + randomNameSuffix();
        try {
            assertUpdate("ALTER TABLE " + sourceSchemaName + "." + tableName + " RENAME TO " + renamedTable);
        }
        catch (Throwable e) {
            try (AutoCloseable ignore = () -> assertUpdate("DROP TABLE " + tableName)) {
                throw e;
            }
        }
        assertQuery("SELECT x FROM " + sourceSchemaName + "." + renamedTable, "VALUES 123");

        assertUpdate("DROP TABLE " + sourceSchemaName + "." + renamedTable);
        assertUpdate("DROP SCHEMA " + sourceSchemaName);
    }

    @Test
    public void testCommentTable()
    {
        if (!hasBehavior(SUPPORTS_COMMENT_ON_TABLE)) {
            assertQueryFails("COMMENT ON TABLE nation IS 'new comment'", "This connector does not support setting table comments");
            return;
        }

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_", "(a integer)")) {
            // comment initially not set
            assertThat(getTableComment(catalogName, schemaName, table.getName())).isEqualTo(null);

            // comment set
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS 'new comment'");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName())).contains("COMMENT 'new comment'");
            assertThat(getTableComment(catalogName, schemaName, table.getName())).isEqualTo("new comment");
            assertThat(query(
                    "SELECT table_name, comment FROM system.metadata.table_comments " +
                            "WHERE catalog_name = '" + catalogName + "' AND schema_name = '" + schemaName + "'")) // without table_name filter
                    .skippingTypesCheck()
                    .containsAll("VALUES ('" + table.getName() + "', 'new comment')");

            // comment deleted
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS NULL");
            assertThat(getTableComment(catalogName, schemaName, table.getName())).isEqualTo(null);
        }

        String tableName = "test_comment_" + randomNameSuffix();
        try {
            // comment set when creating a table
            assertUpdate("CREATE TABLE " + tableName + "(key integer) COMMENT 'new table comment'");
            assertThat(getTableComment(catalogName, schemaName, tableName)).isEqualTo("new table comment");

            // comment set to empty or deleted
            assertUpdate("COMMENT ON TABLE " + tableName + " IS ''");
            assertThat(getTableComment(catalogName, schemaName, tableName)).isIn("", null); // Some storages do not preserve empty comment
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    protected String getTableComment(String catalogName, String schemaName, String tableName)
    {
        String sql = format("SELECT comment FROM system.metadata.table_comments WHERE catalog_name = '%s' AND schema_name = '%s' AND table_name = '%s'", catalogName, schemaName, tableName);
        return (String) computeScalar(sql);
    }

    @Test
    public void testCommentView()
    {
        if (!hasBehavior(SUPPORTS_COMMENT_ON_VIEW)) {
            if (hasBehavior(SUPPORTS_CREATE_VIEW)) {
                try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view", "SELECT * FROM region")) {
                    assertQueryFails("COMMENT ON VIEW " + view.getName() + " IS 'new comment'", "This connector does not support setting view comments");
                }
                return;
            }
            abort("Skipping as connector does not support CREATE VIEW");
        }

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view", "SELECT * FROM region")) {
            // comment set
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'new comment'");
            assertThat((String) computeScalar("SHOW CREATE VIEW " + view.getName())).contains("COMMENT 'new comment'");
            assertThat(getTableComment(catalogName, schemaName, view.getName())).isEqualTo("new comment");

            // comment deleted
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS NULL");
            assertThat(getTableComment(catalogName, schemaName, view.getName())).isEqualTo(null);

            // comment set to non-empty value before verifying setting empty comment
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS 'updated comment'");
            assertThat(getTableComment(catalogName, schemaName, view.getName())).isEqualTo("updated comment");

            // comment set to empty
            assertUpdate("COMMENT ON VIEW " + view.getName() + " IS ''");
            assertThat(getTableComment(catalogName, schemaName, view.getName())).isEqualTo("");
        }

        String viewName = "test_comment_view" + randomNameSuffix();
        try {
            // comment set when creating a table
            assertUpdate("CREATE VIEW " + viewName + " COMMENT 'new view comment' AS SELECT * FROM region");
            assertThat(getTableComment(catalogName, schemaName, viewName)).isEqualTo("new view comment");
        }
        finally {
            assertUpdate("DROP VIEW IF EXISTS " + viewName);
        }
    }

    @Test
    public void testCommentColumn()
    {
        if (!hasBehavior(SUPPORTS_COMMENT_ON_COLUMN)) {
            assertQueryFails("COMMENT ON COLUMN nation.nationkey IS 'new comment'", "This connector does not support setting column comments");
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_column_", "(a integer)")) {
            // comment set
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS 'new comment'");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.getName())).contains("COMMENT 'new comment'");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("new comment");

            // comment deleted
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS NULL");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo(null);

            // comment set to non-empty value before verifying setting empty comment
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS 'updated comment'");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("updated comment");

            // comment set to empty or deleted
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS ''");
            assertThat(getColumnComment(table.getName(), "a")).isIn("", null); // Some storages do not preserve empty comment
        }
    }

    @Test
    public void testCommentColumnName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_COMMENT_ON_COLUMN));

        for (String columnName : testColumnNameDataProvider()) {
            testCommentColumnName(columnName, requiresDelimiting(columnName));
        }
    }

    protected void testCommentColumnName(String columnName, boolean delimited)
    {
        String nameInSql = toColumnNameInSql(columnName, delimited);

        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_column", "(" + nameInSql + " integer)")) {
            assertUpdate("COMMENT ON COLUMN " + table.getName() + "." + nameInSql + " IS 'test comment'");
            assertThat(getColumnComment(table.getName(), columnName.replace("'", "''").toLowerCase(ENGLISH))).isEqualTo("test comment");
        }
        catch (RuntimeException e) {
            if (isColumnNameRejected(e, columnName, delimited)) {
                // It is OK if give column name is not allowed and is clearly rejected by the connector.
                return;
            }
            throw e;
        }
    }

    @Test
    public void testCommentViewColumn()
    {
        if (!hasBehavior(SUPPORTS_COMMENT_ON_VIEW_COLUMN)) {
            if (hasBehavior(SUPPORTS_CREATE_VIEW)) {
                try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view_column", "SELECT * FROM region")) {
                    assertQueryFails("COMMENT ON COLUMN " + view.getName() + ".regionkey IS 'new region key comment'", "This connector does not support setting view column comments");
                }
                return;
            }
            abort("Skipping as connector does not support CREATE VIEW");
        }

        String viewColumnName = "regionkey";
        try (TestView view = new TestView(getQueryRunner()::execute, "test_comment_view_column", "SELECT * FROM region")) {
            // comment set
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'new region key comment'");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("new region key comment");

            // comment deleted
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS NULL");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo(null);

            // comment set to non-empty value before verifying setting empty comment
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS 'updated region key comment'");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("updated region key comment");

            // comment set to empty
            assertUpdate("COMMENT ON COLUMN " + view.getName() + "." + viewColumnName + " IS ''");
            assertThat(getColumnComment(view.getName(), viewColumnName)).isEqualTo("");
        }
    }

    protected String getColumnComment(String tableName, String columnName)
    {
        return (String) computeScalar(format(
                "SELECT comment FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'",
                getSession().getSchema().orElseThrow(),
                tableName,
                columnName));
    }

    @Test
    public void testInsert()
    {
        if (!hasBehavior(SUPPORTS_INSERT)) {
            assertQueryFails("INSERT INTO nation(nationkey) VALUES (42)", "This connector does not support inserts");
            return;
        }

        // We are using SUPPORTS_CREATE_TABLE_WITH_DATA because WITH NO DATA is using same execution code paths
        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA)) {
            throw new AssertionError("Cannot test INSERT without CTAS, the test needs to be implemented in a connector-specific way");
        }

        String query = "SELECT name, nationkey, regionkey FROM nation";

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_", "AS " + query + " WITH NO DATA")) {
            assertQuery("SELECT count(*) FROM " + table.getName() + "", "SELECT 0");

            assertUpdate("INSERT INTO " + table.getName() + " " + query, 25);

            assertQuery("SELECT * FROM " + table.getName() + "", query);

            assertUpdate("INSERT INTO " + table.getName() + " (nationkey) VALUES (-1)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (nationkey) VALUES (null)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (name) VALUES ('name-dummy-1')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (nationkey, name) VALUES (-2, 'name-dummy-2')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (name, nationkey) VALUES ('name-dummy-3', -3)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (regionkey) VALUES (1234)", 1);

            assertQuery("SELECT * FROM " + table.getName() + "", query
                    + " UNION ALL SELECT null, -1, null"
                    + " UNION ALL SELECT null, null, null"
                    + " UNION ALL SELECT 'name-dummy-1', null, null"
                    + " UNION ALL SELECT 'name-dummy-2', -2, null"
                    + " UNION ALL SELECT 'name-dummy-3', -3, null"
                    + " UNION ALL SELECT null, null, 1234");

            // UNION query produces columns in the opposite order
            // of how they are declared in the table schema
            assertUpdate(
                    "INSERT INTO " + table.getName() + " (nationkey, name, regionkey) " +
                            "SELECT nationkey, name, regionkey FROM nation " +
                            "UNION ALL " +
                            "SELECT nationkey, name, regionkey FROM nation",
                    50);
        }
    }

    @Test
    public void testInsertForDefaultColumn()
    {
        skipTestUnless(hasBehavior(SUPPORTS_INSERT));

        try (TestTable testTable = createTableWithDefaultColumns()) {
            assertUpdate(format("INSERT INTO %s (col_required, col_required2) VALUES (1, 10)", testTable.getName()), 1);
            assertUpdate(format("INSERT INTO %s VALUES (2, 3, 4, 5, 6)", testTable.getName()), 1);
            assertUpdate(format("INSERT INTO %s VALUES (7, null, null, 8, 9)", testTable.getName()), 1);
            assertUpdate(format("INSERT INTO %s (col_required2, col_required) VALUES (12, 13)", testTable.getName()), 1);

            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES (1, null, 43, 42, 10), (2, 3, 4, 5, 6), (7, null, null, 8, 9), (13, null, 43, 42, 12)");
        }
    }

    protected TestTable createTableWithDefaultColumns()
    {
        throw new UnsupportedOperationException();
    }

    @Test
    public void testInsertUnicode()
    {
        skipTestUnless(hasBehavior(SUPPORTS_INSERT));
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new AssertionError("Cannot test INSERT without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_unicode_", "(test varchar(50))")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5world\\7F16\\7801' ", 2);
            assertThat(computeActual("SELECT test FROM " + table.getName()).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("Hello", "hello测试world编码");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_unicode_", "(test varchar(50))")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'aa', 'bé'", 2);
            assertQuery("SELECT test FROM " + table.getName(), "VALUES 'aa', 'bé'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test = 'aa'", "VALUES 'aa'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test > 'ba'", "VALUES 'bé'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test < 'ba'", "VALUES 'aa'");
            assertQueryReturnsEmptyResult("SELECT test FROM " + table.getName() + " WHERE test = 'ba'");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_unicode_", "(test varchar(50))")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'a', 'é'", 2);
            assertQuery("SELECT test FROM " + table.getName(), "VALUES 'a', 'é'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test = 'a'", "VALUES 'a'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test > 'b'", "VALUES 'é'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test < 'b'", "VALUES 'a'");
            assertQueryReturnsEmptyResult("SELECT test FROM " + table.getName() + " WHERE test = 'b'");
        }
    }

    @Test
    public void testInsertHighestUnicodeCharacter()
    {
        skipTestUnless(hasBehavior(SUPPORTS_INSERT));
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new AssertionError("Cannot test INSERT without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_unicode_", "(test varchar(50))")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801' ", 2);
            assertThat(computeActual("SELECT test FROM " + table.getName()).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("Hello", "hello测试􏿿world编码");
        }
    }

    @Test
    public void testInsertArray()
    {
        skipTestUnless(hasBehavior(SUPPORTS_INSERT));
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new AssertionError("Cannot test INSERT without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }

        String tableName = "test_insert_array_" + randomNameSuffix();
        if (!hasBehavior(SUPPORTS_ARRAY)) {
            assertThat(query("CREATE TABLE " + tableName + " (a array(bigint))"))
                    // TODO Unify failure message across connectors
                    .failure().hasMessageMatching("[Uu]nsupported (column )?type: \\Qarray(bigint)");
            abort("not supported");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_array_", "(a ARRAY<DOUBLE>, b ARRAY<BIGINT>)")) {
            assertUpdate("INSERT INTO " + table.getName() + " (a) VALUES (ARRAY[null])", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (a, b) VALUES (ARRAY[1.23E1], ARRAY[1.23E1])", 1);
            assertQuery("SELECT a[1], b[1] FROM " + table.getName(), "VALUES (null, null), (12.3, 12)");
        }
    }

    @Test
    public void testInsertMap()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_INSERT));

        String tableName = "test_insert_map_" + randomNameSuffix();
        if (!hasBehavior(SUPPORTS_MAP_TYPE)) {
            assertThat(query("CREATE TABLE " + tableName + " (a map(integer, integer))"))
                    // TODO Unify failure message across connectors
                    .failure().hasMessageMatching("[Uu]nsupported (column )?type: \\Qmap(integer, integer)");
            abort("not supported");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_map_", "(col map(integer, integer))")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES map(ARRAY[1], ARRAY[2])", 1);
            assertThat(query("SELECT * FROM " + table.getName()))
                .matches("VALUES map(ARRAY[1], ARRAY[2])");
        }
    }

    @Test
    public void testInsertSameValues()
    {
        skipTestUnless(hasBehavior(SUPPORTS_INSERT));
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "insert_same_values",
                "AS " + join(" UNION ALL ", nCopies(2, "SELECT * FROM region")))) {
            assertQuery("SELECT count(*) FROM " + table.getName(), "VALUES 10");
        }
    }

    @Test
    public void testInsertNegativeDate()
    {
        if (!hasBehavior(SUPPORTS_INSERT)) {
            assertQueryFails("INSERT INTO orders (orderdate) VALUES (DATE '-0001-01-01')", "This connector does not support inserts");
            return;
        }
        if (!hasBehavior(SUPPORTS_CREATE_TABLE)) {
            throw new AssertionError("Cannot test INSERT negative dates without CREATE TABLE, the test needs to be implemented in a connector-specific way");
        }
        if (!hasBehavior(SUPPORTS_NEGATIVE_DATE)) {
            try (TestTable table = new TestTable(getQueryRunner()::execute, "insert_date", "(dt DATE)")) {
                assertQueryFails(format("INSERT INTO %s VALUES (DATE '-0001-01-01')", table.getName()), errorMessageForInsertNegativeDate("-0001-01-01"));
            }
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "insert_date", "(dt DATE)")) {
            assertUpdate(format("INSERT INTO %s VALUES (DATE '-0001-01-01')", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES DATE '-0001-01-01'");
            assertQuery(format("SELECT * FROM %s WHERE dt = DATE '-0001-01-01'", table.getName()), "VALUES DATE '-0001-01-01'");
        }
    }

    @Language("RegExp")
    protected String errorMessageForInsertNegativeDate(String date)
    {
        throw new UnsupportedOperationException("This method should be overridden");
    }

    @Test
    public void testInsertIntoNotNullColumn()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        if (!hasBehavior(SUPPORTS_NOT_NULL_CONSTRAINT)) {
            assertQueryFails(
                    "CREATE TABLE not_null_constraint (not_null_col INTEGER NOT NULL)",
                    format("line 1:35: Catalog '%s' does not support non-null column for column name 'not_null_col'", getSession().getCatalog().orElseThrow()));
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "insert_not_null", "(nullable_col INTEGER, not_null_col INTEGER NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            // The error message comes from remote databases when ConnectorMetadata.supportsMissingColumnsOnInsert is true
            assertQueryFails(format("INSERT INTO %s (nullable_col) VALUES (1)", table.getName()), errorMessageForInsertIntoNotNullColumn("not_null_col"));
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (NULL, 3)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (TRY(5/0), 4)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            assertQueryFails(format("INSERT INTO %s (not_null_col) VALUES (TRY(6/0))", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
            assertQueryFails(format("INSERT INTO %s (nullable_col) SELECT nationkey FROM nation", table.getName()), errorMessageForInsertIntoNotNullColumn("not_null_col"));
            assertUpdate(format("INSERT INTO %s (nullable_col) SELECT nationkey FROM nation WHERE regionkey < 0", table.getName()), 0);
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "commuted_not_null", "(nullable_col BIGINT, not_null_col BIGINT NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            // This is enforced by the engine and not the connector
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (NULL, 3)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
        }
    }

    @Test
    public void testUpdateNotNullColumn()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        skipTestUnless(hasBehavior(SUPPORTS_UPDATE));

        if (!hasBehavior(SUPPORTS_NOT_NULL_CONSTRAINT)) {
            assertQueryFails(
                    "CREATE TABLE not_null_constraint (not_null_col INTEGER NOT NULL)",
                    format("line 1:35: Catalog '%s' does not support non-null column for column name 'not_null_col'", getSession().getCatalog().orElseThrow()));
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "update_not_null", "(nullable_col INTEGER, not_null_col INTEGER NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (nullable_col, not_null_col) VALUES (1, 10)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 10)");
            assertQueryFails("UPDATE " + table.getName() + " SET not_null_col = NULL WHERE nullable_col = 1", "NULL value not allowed for NOT NULL column: not_null_col");
            assertQueryFails("UPDATE " + table.getName() + " SET not_null_col = TRY(5/0) where nullable_col = 1", "NULL value not allowed for NOT NULL column: not_null_col");
        }
    }

    @Language("RegExp")
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        throw new UnsupportedOperationException("This method should be overridden");
    }

    @Test
    public void testInsertInTransaction()
    {
        skipTestUnless(hasBehavior(SUPPORTS_INSERT));
        skipTestUnless(hasBehavior(SUPPORTS_MULTI_STATEMENT_WRITES)); // covered by testWriteNotAllowedInTransaction

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_tx_insert",
                "(a bigint)")) {
            String tableName = table.getName();
            inTransaction(session -> assertUpdate(session, "INSERT INTO " + tableName + " VALUES 42", 1));
            assertQuery("TABLE " + tableName, "VALUES 42");
        }
    }

    @Test
    public void testSelectAfterInsertInTransaction()
    {
        if (!hasBehavior(SUPPORTS_INSERT) || !hasBehavior(SUPPORTS_MULTI_STATEMENT_WRITES)) {
            // nothing to test
            log.info("Connector does not support insert in transaction context, so nothing to test");
            return;
        }

        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_insert_select_",
                "AS SELECT nationkey, name, regionkey FROM nation WHERE nationkey = 1")) {
            String tableName = table.getName();
            boolean commit;
            try {
                inTransaction(session -> {
                    // SELECT first, to prime transactional caches, if any
                    assertQuery(session, "TABLE " + tableName, "SELECT nationkey, name, regionkey FROM nation WHERE nationkey = 1");
                    // INSERT
                    assertUpdate(session, "INSERT INTO " + tableName + "(nationkey, name, regionkey) SELECT nationkey, name, regionkey FROM nation WHERE nationkey = 2", 1);
                    // SELECT again
                    try {
                        assertQuery(session, "TABLE " + tableName, "SELECT nationkey, name, regionkey FROM nation WHERE nationkey IN (1, 2)");
                    }
                    catch (Throwable e) {
                        verifySelectAfterInsertFailurePermissible(e);
                        throw new RollbackException();
                    }
                });
                commit = true;
            }
            catch (RollbackException _) {
                // failure accepted, transaction rolled back
                commit = false;
            }
            // SELECT again after transaction completes
            assertQuery(
                    "TABLE " + tableName,
                    "SELECT nationkey, name, regionkey FROM nation WHERE nationkey IN " + (commit ? "(1, 2)" : "(1)"));
        }
    }

    protected void verifySelectAfterInsertFailurePermissible(Throwable e)
    {
        fail("Unexpected failure", e);
    }

    @Test
    public void testDelete()
    {
        skipTestUnless(hasBehavior(SUPPORTS_DELETE));

        // delete successive parts of the table
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_", "AS SELECT * FROM orders")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE custkey <= 100", "SELECT count(*) FROM orders WHERE custkey <= 100");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM orders WHERE custkey > 100");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE custkey <= 300", "SELECT count(*) FROM orders WHERE custkey > 100 AND custkey <= 300");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM orders WHERE custkey > 300");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE custkey <= 500", "SELECT count(*) FROM orders WHERE custkey > 300 AND custkey <= 500");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM orders WHERE custkey > 500");
        }

        // delete without matching any rows
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_", "AS SELECT * FROM orders")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE orderkey < 0", 0);
        }

        // delete with a predicate that optimizes to false
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_", "AS SELECT * FROM orders")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE orderkey > 5 AND orderkey < 4", 0);
        }

        String tableName = "test_delete_" + randomNameSuffix();
        try {
            // test EXPLAIN ANALYZE with CTAS
            assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE " + tableName + " AS SELECT CAST(orderstatus AS VARCHAR(15)) orderstatus FROM orders");
            assertQuery("SELECT * from " + tableName, "SELECT orderstatus FROM orders");
            // check that INSERT works also
            assertExplainAnalyze("EXPLAIN ANALYZE INSERT INTO " + tableName + " SELECT clerk FROM orders");
            assertQuery("SELECT * from " + tableName, "SELECT orderstatus FROM orders UNION ALL SELECT clerk FROM orders");
            // check DELETE works with EXPLAIN ANALYZE
            assertExplainAnalyze("EXPLAIN ANALYZE DELETE FROM " + tableName + " WHERE TRUE");
            assertQuery("SELECT COUNT(*) from " + tableName, "SELECT 0");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testDeleteWithLike()
    {
        skipTestUnless(hasBehavior(SUPPORTS_DELETE));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_with_like_", "AS SELECT * FROM nation")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE name LIKE '%a%'", "VALUES 0");
            assertUpdate("DELETE FROM " + table.getName() + " WHERE name LIKE '%A%'", "SELECT count(*) FROM nation WHERE name LIKE '%A%'");
        }
    }

    @Test
    public void testDeleteWithComplexPredicate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_DELETE));

        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_complex_", "AS SELECT * FROM nation")) {
            // delete half the table, then delete the rest
            assertUpdate("DELETE FROM " + table.getName() + " WHERE nationkey % 2 = 0", "SELECT count(*) FROM nation WHERE nationkey % 2 = 0");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM nation WHERE nationkey % 2 <> 0");

            assertUpdate("DELETE FROM " + table.getName(), "SELECT count(*) FROM nation WHERE nationkey % 2 <> 0");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM orders LIMIT 0");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE rand() < 0", 0);
        }
    }

    @Test
    public void testDeleteWithSubquery()
    {
        // TODO (https://github.com/trinodb/trino/issues/13210) Migrate these tests to AbstractTestEngineOnlyQueries
        skipTestUnless(hasBehavior(SUPPORTS_DELETE));

        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_subquery", "AS SELECT * FROM nation")) {
            // delete using a subquery
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey IN (SELECT regionkey FROM region WHERE name LIKE 'A%')", 15);
            assertQuery(
                    "SELECT * FROM " + table.getName(),
                    "SELECT * FROM nation WHERE regionkey IN (SELECT regionkey FROM region WHERE name NOT LIKE 'A%')");
        }

        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_subquery", "AS SELECT * FROM orders")) {
            // delete using a scalar and EXISTS subquery
            assertUpdate("DELETE FROM " + table.getName() + " WHERE orderkey = (SELECT orderkey FROM orders ORDER BY orderkey LIMIT 1)", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE orderkey = (SELECT orderkey FROM orders WHERE false)", 0);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE EXISTS(SELECT 1 WHERE false)", 0);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE EXISTS(SELECT 1)", "SELECT count(*) - 1 FROM orders");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_correlated_exists_subquery", "AS SELECT * FROM nation")) {
            // delete using correlated EXISTS subquery
            assertUpdate(format("DELETE FROM %1$s WHERE EXISTS(SELECT regionkey FROM region WHERE regionkey = %1$s.regionkey AND name LIKE 'A%%')", table.getName()), 15);
            assertQuery(
                    "SELECT * FROM " + table.getName(),
                    "SELECT * FROM nation WHERE regionkey IN (SELECT regionkey FROM region WHERE name NOT LIKE 'A%')");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_correlated_exists_subquery", "AS SELECT * FROM nation")) {
            // delete using correlated IN subquery
            assertUpdate(format("DELETE FROM %1$s WHERE regionkey IN (SELECT regionkey FROM region WHERE regionkey = %1$s.regionkey AND name LIKE 'A%%')", table.getName()), 15);
            assertQuery(
                    "SELECT * FROM " + table.getName(),
                    "SELECT * FROM nation WHERE regionkey IN (SELECT regionkey FROM region WHERE name NOT LIKE 'A%')");
        }
    }

    @Test
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        skipTestUnless(hasBehavior(SUPPORTS_DELETE));

        String tableName = "test_delete_" + randomNameSuffix();

        // delete using a subquery
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", 25);
        assertExplainAnalyze("EXPLAIN ANALYZE DELETE FROM " + tableName + " WHERE regionkey IN (SELECT regionkey FROM region WHERE name LIKE 'A%' LIMIT 1)",
                "SemiJoin.*");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDeleteWithSemiJoin()
    {
        skipTestUnless(hasBehavior(SUPPORTS_DELETE));

        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_semijoin", "AS SELECT * FROM nation")) {
            // delete with multiple SemiJoin
            assertUpdate(
                    "DELETE FROM " + table.getName() + " " +
                            "WHERE regionkey IN (SELECT regionkey FROM region WHERE name LIKE 'A%') " +
                            "  AND regionkey IN (SELECT regionkey FROM region WHERE length(comment) < 50)",
                    10);
            assertQuery(
                    "SELECT * FROM " + table.getName(),
                    "SELECT * FROM nation " +
                            "WHERE regionkey IN (SELECT regionkey FROM region WHERE name NOT LIKE 'A%') " +
                            "  OR regionkey IN (SELECT regionkey FROM region WHERE length(comment) >= 50)");
        }

        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_semijoin", "AS SELECT * FROM orders")) {
            // delete with SemiJoin null handling
            assertUpdate(
                    "DELETE FROM " + table.getName() + "\n" +
                            "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM tpch.tiny.lineitem)) IS NULL\n",
                    "SELECT count(*) FROM orders\n" +
                            "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NULL\n");
            assertQuery(
                    "SELECT * FROM " + table.getName(),
                    "SELECT * FROM orders\n" +
                            "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NOT NULL\n");
        }
    }

    @Test
    public void testDeleteWithVarcharPredicate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_DELETE));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_with_varchar_predicate_", "AS SELECT * FROM orders")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE orderstatus = 'O'", "SELECT count(*) FROM orders WHERE orderstatus = 'O'");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM orders WHERE orderstatus <> 'O'");
        }
    }

    @Test
    public void verifySupportsDeleteDeclaration()
    {
        if (hasBehavior(SUPPORTS_DELETE)) {
            // Covered by testDeleteAllDataFromTable
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_supports_delete", "(regionkey int)")) {
            assertQueryFails("DELETE FROM " + table.getName(), MODIFYING_ROWS_MESSAGE);
        }
    }

    @Test
    public void verifySupportsRowLevelDeleteDeclaration()
    {
        if (hasBehavior(SUPPORTS_ROW_LEVEL_DELETE)) {
            // Covered by testRowLevelDelete
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_supports_row_level_delete", "(regionkey int)")) {
            assertQueryFails("DELETE FROM " + table.getName() + " WHERE regionkey = 2", MODIFYING_ROWS_MESSAGE);
        }
    }

    @Test
    public void testDeleteAllDataFromTable()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_DELETE));
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_all_data", "AS SELECT * FROM region")) {
            // not using assertUpdate as some connectors provide update count and some not
            getQueryRunner().execute("DELETE FROM " + table.getName());
            assertQuery("SELECT count(*) FROM " + table.getName(), "VALUES 0");
        }
    }

    @Test
    public void testRowLevelDelete()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_ROW_LEVEL_DELETE));
        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_row_delete", "AS SELECT * FROM region")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 2", 1);
            assertQuery("SELECT count(*) FROM " + table.getName(), "VALUES 4");
        }
    }

    @Test
    public void verifySupportsUpdateDeclaration()
    {
        if (hasBehavior(SUPPORTS_UPDATE)) {
            // Covered by testUpdate
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_supports_update", "AS SELECT * FROM nation")) {
            assertQueryFails("UPDATE " + table.getName() + " SET nationkey = 100 WHERE regionkey = 2", MODIFYING_ROWS_MESSAGE);
        }
    }

    @Test
    public void verifySupportsRowLevelUpdateDeclaration()
    {
        if (hasBehavior(SUPPORTS_ROW_LEVEL_UPDATE)) {
            // Covered by testRowLevelUpdate
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_supports_update", "AS SELECT * FROM nation")) {
            assertQueryFails("UPDATE " + table.getName() + " SET nationkey = nationkey * 100 WHERE regionkey = 2", MODIFYING_ROWS_MESSAGE);
        }
    }

    @Test
    public void testUpdate()
    {
        if (!hasBehavior(SUPPORTS_UPDATE)) {
            // Note this change is a no-op, if actually run
            assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1", MODIFYING_ROWS_MESSAGE);
            return;
        }
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_row_update", "AS SELECT * FROM nation")) {
            assertUpdate("UPDATE " + table.getName() + " SET nationkey = 100 WHERE regionkey = 2", 5);
            assertQuery("SELECT count(*) FROM " + table.getName() + " WHERE nationkey = 100", "VALUES 5");
        }
    }

    @Test
    public void testUpdateMultipleCondition()
    {
        skipTestUnless(hasBehavior(SUPPORTS_UPDATE));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_row_update", "AS SELECT * FROM (VALUES (1, 10), (1, 20), (2, 10)) AS t(a, b)")) {
            assertUpdate("UPDATE " + table.getName() + " SET b = 100 WHERE a = 1 AND b = 10", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (1, 100), (1, 20), (2, 10)");
        }
    }

    @Test
    public void testRowLevelUpdate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));
        if (!hasBehavior(SUPPORTS_ROW_LEVEL_UPDATE)) {
            // Note this change is a no-op, if actually run
            assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1", MODIFYING_ROWS_MESSAGE);
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_update", "AS TABLE tpch.tiny.nation")) {
            String tableName = table.getName();
            assertUpdate("UPDATE " + tableName + " SET nationkey = 100 + nationkey WHERE regionkey = 2", 5);
            assertThat(query("SELECT * FROM " + tableName))
                    .skippingTypesCheck()
                    .matches("SELECT IF(regionkey=2, nationkey + 100, nationkey) nationkey, name, regionkey, comment FROM tpch.tiny.nation");

            // UPDATE after UPDATE
            assertUpdate("UPDATE " + tableName + " SET nationkey = nationkey * 2 WHERE regionkey IN (2,3)", 10);
            assertThat(query("SELECT * FROM " + tableName))
                    .skippingTypesCheck()
                    .matches("SELECT CASE regionkey WHEN 2 THEN 2*(nationkey+100) WHEN 3 THEN 2*nationkey ELSE nationkey END nationkey, name, regionkey, comment FROM tpch.tiny.nation");
        }
    }

    @Test
    public void testUpdateCaseSensitivity()
    {
        skipTestUnless(hasBehavior(SUPPORTS_UPDATE));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_row_update", "AS SELECT * FROM nation")) {
            assertUpdate("UPDATE " + table.getName() + " SET NATIONKEY = 100 WHERE REGIONKEY = 2", 5);
            assertQuery("SELECT count(*) FROM " + table.getName() + " WHERE nationkey = 100", "VALUES 5");
        }
    }

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @RepeatedTest(4)
    @Timeout(60)
    public void testUpdateRowConcurrently()
            throws Exception
    {
        if (!hasBehavior(SUPPORTS_UPDATE)) {
            // Covered by testUpdate
            return;
        }

        int threads = 4;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "test_concurrent_update",
                IntStream.range(0, threads)
                        .mapToObj(i -> format("col%s integer", i))
                        .collect(joining(", ", "(", ")")))) {
            String tableName = table.getName();
            assertUpdate(format("INSERT INTO %s VALUES (%s)", tableName, join(",", nCopies(threads, "0"))), 1);

            List<Future<Boolean>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            String columnName = "col" + threadNumber;
                            getQueryRunner().execute(format("UPDATE %s SET %s = %s + 1", tableName, columnName, columnName));
                            return true;
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                verifyConcurrentUpdateFailurePermissible(trinoException);
                            }
                            catch (Throwable verifyFailure) {
                                if (verifyFailure != e) {
                                    verifyFailure.addSuppressed(e);
                                }
                                throw verifyFailure;
                            }
                            return false;
                        }
                    }))
                    .collect(toImmutableList());

            String expected = futures.stream()
                    .map(future -> tryGetFutureValue(future, 10, SECONDS).orElseThrow(() -> new RuntimeException("Wait timed out")))
                    .map(success -> success ? "1" : "0")
                    .collect(joining(",", "VALUES (", ")"));

            assertThat(query("TABLE " + tableName))
                    .matches(expected);
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }
    }

    protected void verifyConcurrentUpdateFailurePermissible(Exception e)
    {
        // By default, do not expect UPDATE to fail in case of concurrent updates
        throw new AssertionError("Unexpected concurrent update failure", e);
    }

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @RepeatedTest(4)
    @Timeout(60)
    public void testInsertRowConcurrently()
            throws Exception
    {
        if (!hasBehavior(SUPPORTS_INSERT)) {
            // Covered by testInsert
            return;
        }

        int threads = 4;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        try (TestTable table = createTableWithOneIntegerColumn("test_insert")) {
            String tableName = table.getName();

            List<Future<OptionalInt>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(10, SECONDS);
                        try {
                            getQueryRunner().execute("INSERT INTO " + tableName + " VALUES (" + threadNumber + ")");
                            return OptionalInt.of(threadNumber);
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                verifyConcurrentInsertFailurePermissible(trinoException);
                            }
                            catch (Throwable verifyFailure) {
                                if (verifyFailure != e) {
                                    verifyFailure.addSuppressed(e);
                                }
                                throw verifyFailure;
                            }
                            return OptionalInt.empty();
                        }
                    }))
                    .collect(toImmutableList());

            List<Integer> values = futures.stream()
                    .map(future -> tryGetFutureValue(future, 10, SECONDS).orElseThrow(() -> new RuntimeException("Wait timed out")))
                    .filter(OptionalInt::isPresent)
                    .map(OptionalInt::getAsInt)
                    .collect(toImmutableList());

            if (values.isEmpty()) {
                assertQueryReturnsEmptyResult("TABLE " + tableName);
            }
            else {
                // Cast to integer because some connectors (e.g. Oracle) map integer to different types that skippingTypesCheck can't resolve the mismatch.
                assertThat(query("SELECT CAST(col AS INTEGER) FROM " + tableName))
                        .matches(values.stream()
                                .map(value -> format("(%s)", value))
                                .collect(joining(",", "VALUES ", "")));
            }
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(10, SECONDS);
        }
    }

    protected void verifyConcurrentInsertFailurePermissible(Exception e)
    {
        // By default, do not expect INSERT to fail in case of concurrent inserts
        throw new AssertionError("Unexpected concurrent insert failure", e);
    }

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @RepeatedTest(4)
    @Timeout(60)
    public void testAddColumnConcurrently()
            throws Exception
    {
        if (!hasBehavior(SUPPORTS_ADD_COLUMN)) {
            // Covered by testAddColumn
            return;
        }

        int threads = 4;
        CyclicBarrier barrier = new CyclicBarrier(threads);
        ExecutorService executor = newFixedThreadPool(threads);
        try (TestTable table = createTableWithOneIntegerColumn("test_add_column")) {
            String tableName = table.getName();

            List<Future<Optional<String>>> futures = IntStream.range(0, threads)
                    .mapToObj(threadNumber -> executor.submit(() -> {
                        barrier.await(30, SECONDS);
                        try {
                            String columnName = "col" + threadNumber;
                            getQueryRunner().execute("ALTER TABLE " + tableName + " ADD COLUMN " + columnName + " integer");
                            return Optional.of(columnName);
                        }
                        catch (Exception e) {
                            RuntimeException trinoException = getTrinoExceptionCause(e);
                            try {
                                verifyConcurrentAddColumnFailurePermissible(trinoException);
                            }
                            catch (Throwable verifyFailure) {
                                if (verifyFailure != e) {
                                    verifyFailure.addSuppressed(e);
                                }
                                throw verifyFailure;
                            }
                            return Optional.<String>empty();
                        }
                    }))
                    .collect(toImmutableList());

            List<String> addedColumns = futures.stream()
                    .map(future -> tryGetFutureValue(future, 30, SECONDS).orElseThrow(() -> new RuntimeException("Wait timed out")))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(toImmutableList());

            assertThat(query("DESCRIBE " + tableName))
                    .result()
                    .projected("Column")
                    .skippingTypesCheck()
                    .matches(Stream.concat(Stream.of("col"), addedColumns.stream())
                            .map(value -> format("'%s'", value))
                            .collect(joining(",", "VALUES ", "")));
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(30, SECONDS);
        }
    }

    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        // By default, do not expect ALTER TABLE ADD COLUMN to fail in case of concurrent inserts
        throw new AssertionError("Unexpected concurrent add column failure", e);
    }

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @RepeatedTest(4)
    @Timeout(60)
    public void testCreateOrReplaceTableConcurrently()
            throws Exception
    {
        if (!hasBehavior(SUPPORTS_CREATE_OR_REPLACE_TABLE)) {
            // Already handled in testCreateOrReplaceTableWhenTableDoesNotExist
            return;
        }

        int threads = 4;
        int numOfCreateOrReplaceStatements = 4;
        int numOfReads = 16;
        CyclicBarrier barrier = new CyclicBarrier(threads + 1);
        ExecutorService executor = newFixedThreadPool(threads + 1);
        List<Future<?>> futures = new ArrayList<>();
        try (TestTable table = createTableWithOneIntegerColumn("test_create_or_replace")) {
            String tableName = table.getName();

            getQueryRunner().execute("CREATE OR REPLACE TABLE " + tableName + " AS SELECT 1 a");
            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1");

            /// One thread submits some CREATE OR REPLACE statements
            futures.add(executor.submit(() -> {
                barrier.await(30, SECONDS);
                IntStream.range(0, numOfCreateOrReplaceStatements).forEach(index -> {
                    try {
                        getQueryRunner().execute("CREATE OR REPLACE TABLE " + tableName + " AS SELECT * FROM (VALUES (1), (2)) AS t(a) ");
                    } catch (Exception e) {
                        RuntimeException trinoException = getTrinoExceptionCause(e);
                        try {
                            throw new AssertionError("Unexpected concurrent CREATE OR REPLACE failure", trinoException);
                        } catch (Throwable verifyFailure) {
                            if (verifyFailure != e) {
                                verifyFailure.addSuppressed(e);
                            }
                            throw verifyFailure;
                        }
                    }
                });
                return null;
            }));
            // Other 4 threads continue try to read the same table, none of the reads should fail.
            IntStream.range(0, threads)
                    .forEach(threadNumber -> futures.add(executor.submit(() -> {
                        barrier.await(30, SECONDS);
                        IntStream.range(0, numOfReads).forEach(readIndex -> {
                            try {
                                MaterializedResult result = computeActual("SELECT * FROM " + tableName);
                                if (result.getRowCount() == 1) {
                                    assertEqualsIgnoreOrder(result.getMaterializedRows(), List.of(new MaterializedRow(List.of(1))));
                                }
                                else {
                                    assertEqualsIgnoreOrder(result.getMaterializedRows(), List.of(new MaterializedRow(List.of(1)), new MaterializedRow(List.of(2))));
                                }
                            }
                            catch (Exception e) {
                                RuntimeException trinoException = getTrinoExceptionCause(e);
                                try {
                                    throw new AssertionError("Unexpected concurrent CREATE OR REPLACE failure", trinoException);
                                }
                                catch (Throwable verifyFailure) {
                                    if (verifyFailure != e) {
                                        verifyFailure.addSuppressed(e);
                                    }
                                    throw verifyFailure;
                                }
                            }
                        });
                        return null;
                    })));
            futures.forEach(Futures::getUnchecked);
            getQueryRunner().execute("CREATE OR REPLACE TABLE " + tableName + " AS SELECT * FROM (VALUES (1), (2), (3)) AS t(a)");
            assertThat(query("SELECT * FROM " + tableName)).matches("VALUES 1, 2, 3");
        }
        finally {
            executor.shutdownNow();
            executor.awaitTermination(30, SECONDS);
        }
    }

    protected TestTable createTableWithOneIntegerColumn(String namePrefix)
    {
        return new TestTable(getQueryRunner()::execute, namePrefix, "(col integer)");
    }

    @Test
    public void testUpdateWithPredicates()
    {
        if (!hasBehavior(SUPPORTS_UPDATE)) {
            // Note this change is a no-op, if actually run
            assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1", MODIFYING_ROWS_MESSAGE);
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_update_with_predicates", "(a INT, b INT, c INT)")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 2, 3), (11, 12, 13), (21, 22, 23)", 3);
            assertUpdate("UPDATE " + tableName + " SET a = a - 1 WHERE c = 3", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (0, 2, 3), (11, 12, 13), (21, 22, 23)");

            assertUpdate("UPDATE " + tableName + " SET c = c + 1 WHERE a = 11", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (0, 2, 3), (11, 12, 14), (21, 22, 23)");

            assertUpdate("UPDATE " + tableName + " SET b = b * 2 WHERE b = 22", 1);
            assertQuery("SELECT * FROM " + tableName, "VALUES (0, 2, 3), (11, 12, 14), (21, 44, 23)");
        }
    }

    @Test
    public void testUpdateRowType()
    {
        if (!hasBehavior(SUPPORTS_UPDATE)) {
            // Note this change is a no-op, if actually run
            assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1", MODIFYING_ROWS_MESSAGE);
            return;
        }

        if (!hasBehavior(SUPPORTS_ROW_TYPE)) {
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_update_with_predicates_on_row_types", "(int_t INT, row_t ROW(f1 INT, f2 INT))")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, ROW(2, 3)), (11, ROW(12, 13)), (21, ROW(22, 23))", 3);
            assertUpdate("UPDATE " + tableName + " SET int_t = int_t - 1 WHERE row_t.f2 = 3", 1);
            assertQuery("SELECT int_t, row_t.f1, row_t.f2 FROM " + tableName, "VALUES (0, 2, 3), (11, 12, 13), (21, 22, 23)");

            assertUpdate("UPDATE " + tableName + " SET row_t = ROW(row_t.f1, row_t.f2 + 1) WHERE int_t = 11", 1);
            assertQuery("SELECT int_t, row_t.f1, row_t.f2 FROM " + tableName, "VALUES (0, 2, 3), (11, 12, 14), (21, 22, 23)");

            assertUpdate("UPDATE " + tableName + " SET row_t = ROW(row_t.f1 * 2, row_t.f2) WHERE row_t.f1 = 22", 1);
            assertQuery("SELECT int_t, row_t.f1, row_t.f2 FROM " + tableName, "VALUES (0, 2, 3), (11, 12, 14), (21, 44, 23)");
        }
    }

    @Test
    public void testPredicateOnRowTypeField()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_INSERT) && hasBehavior(SUPPORTS_ROW_TYPE));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_predicate_on_row_type_field", "(int_t INT, row_t row(varchar_t VARCHAR, int_t INT))")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (2, row('first', 1)), (20, row('second', 10)), (200, row('third', 100))", 3);
            assertQuery("SELECT int_t FROM " + table.getName() + " WHERE row_t.int_t = 1", "VALUES 2");
            assertQuery("SELECT int_t FROM " + table.getName() + " WHERE row_t.int_t > 1", "VALUES 20, 200");
            assertQuery("SELECT int_t FROM " + table.getName() + " WHERE int_t = 2 AND row_t.int_t = 1", "VALUES 2");
        }
    }

    @Test
    public void testUpdateAllValues()
    {
        if (!hasBehavior(SUPPORTS_UPDATE)) {
            // Note this change is a no-op, if actually run
            assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1", MODIFYING_ROWS_MESSAGE);
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_update_all_columns", "(a INT, b INT, c INT)")) {
            String tableName = table.getName();
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 2, 3), (11, 12, 13), (21, 22, 23)", 3);
            assertUpdate("UPDATE " + tableName + " SET a = a + 1, b = b - 1, c = c * 2", 3);
            assertQuery("SELECT * FROM " + tableName, "VALUES (2, 1, 6), (12, 11, 26), (22, 21, 46)");
        }
    }

    @Test
    public void testDropTable()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        String tableName = "test_drop_table_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(col bigint)");
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isTrue();

        assertUpdate("DROP TABLE " + tableName);
        assertThat(getQueryRunner().tableExists(getSession(), tableName)).isFalse();
    }

    @Test
    public void testDropTableIfExists()
    {
        assertThat(getQueryRunner().tableExists(getSession(), "test_drop_if_exists")).isFalse();
        assertUpdate("DROP TABLE IF EXISTS test_drop_if_exists");
        assertThat(getQueryRunner().tableExists(getSession(), "test_drop_if_exists")).isFalse();
    }

    @Test
    public void testTruncateTable()
    {
        if (!hasBehavior(SUPPORTS_TRUNCATE)) {
            assertQueryFails("TRUNCATE TABLE nation", "This connector does not support truncating tables");
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_truncate", "AS SELECT * FROM region")) {
            assertUpdate("TRUNCATE TABLE " + table.getName());
            assertQuery("SELECT count(*) FROM " + table.getName(), "VALUES 0");
        }
    }

    @Test
    public void testQueryLoggingCount()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        QueryManager queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();
        executeExclusively(() -> {
            assertEventually(
                    new Duration(1, MINUTES),
                    () -> assertThat(queryManager.getQueries().stream()
                            .map(BasicQueryInfo::getQueryId)
                            .map(queryManager::getFullQueryInfo)
                            .filter(info -> !info.isFinalQueryInfo())
                            .collect(toList())).isEqualTo(ImmutableList.of()));

            // We cannot simply get the number of completed queries as soon as all the queries are completed, because this counter may not be up-to-date at that point.
            // The completed queries counter is updated in a final query info listener, which is called eventually.
            // Therefore, here we wait until the value of this counter gets stable.

            DispatchManager dispatchManager = getQueryRunner().getCoordinator().getDispatchManager();
            long beforeCompletedQueriesCount = waitUntilStable(() -> dispatchManager.getStats().getCompletedQueries().getTotalCount(), new Duration(5, SECONDS));
            long beforeSubmittedQueriesCount = dispatchManager.getStats().getSubmittedQueries().getTotalCount();
            String tableName = "test_logging_count" + randomNameSuffix();
            assertUpdate("CREATE TABLE " + tableName + tableDefinitionForQueryLoggingCount());
            assertQueryReturnsEmptyResult("SELECT foo_1, foo_2_4 FROM " + tableName);
            assertUpdate("DROP TABLE " + tableName);
            assertQueryFails("SELECT * FROM " + tableName, ".*Table .* does not exist");

            // TODO: Figure out a better way of synchronization
            assertEventually(
                    new Duration(1, MINUTES),
                    () -> assertThat(dispatchManager.getStats().getCompletedQueries().getTotalCount() - beforeCompletedQueriesCount).isEqualTo(4));
            assertThat(dispatchManager.getStats().getSubmittedQueries().getTotalCount() - beforeSubmittedQueriesCount).isEqualTo(4);
        });
    }

    /**
     * The table must have two columns foo_1 and foo_2_4 of any type.
     */
    @Language("SQL")
    protected String tableDefinitionForQueryLoggingCount()
    {
        return "(foo_1 int, foo_2_4 int)";
    }

    private <T> T waitUntilStable(Supplier<T> computation, Duration timeout)
    {
        T lastValue = computation.get();
        long start = System.nanoTime();
        while (!currentThread().isInterrupted() && nanosSince(start).compareTo(timeout) < 0) {
            sleepUninterruptibly(100, MILLISECONDS);
            T currentValue = computation.get();
            if (currentValue.equals(lastValue)) {
                return currentValue;
            }
            lastValue = currentValue;
        }
        throw new UncheckedTimeoutException();
    }

    @Test
    public void testShowSchemasFromOther()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS FROM tpch");
        assertThat(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of(INFORMATION_SCHEMA, "tiny", "sf1"))).isTrue();
    }

    // TODO move to to engine-only
    @Test
    public void testSymbolAliasing()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        String tableName = "test_symbol_aliasing" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 foo_1, 2 foo_2_4", 1);
        assertQuery("SELECT foo_1, foo_2_4 FROM " + tableName, "SELECT 1, 2");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testWrittenStats()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        skipTestUnless(hasBehavior(SUPPORTS_INSERT));

        String tableName = "test_written_stats_" + randomNameSuffix();
        try {
            String sql = "CREATE TABLE " + tableName + " AS SELECT * FROM nation";
            MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
            QueryInfo queryInfo = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId());

            assertThat(queryInfo.getQueryStats().getOutputPositions()).isEqualTo(1L);
            assertThat(queryInfo.getQueryStats().getWrittenPositions()).isEqualTo(25L);
            assertThat(queryInfo.getQueryStats().getLogicalWrittenDataSize().toBytes() > 0L).isTrue();

            sql = "INSERT INTO " + tableName + " SELECT * FROM nation LIMIT 10";
            result = getDistributedQueryRunner().executeWithPlan(getSession(), sql);
            queryInfo = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId());

            assertThat(queryInfo.getQueryStats().getOutputPositions()).isEqualTo(1L);
            assertThat(queryInfo.getQueryStats().getWrittenPositions()).isEqualTo(10L);
            assertThat(queryInfo.getQueryStats().getLogicalWrittenDataSize().toBytes() > 0L).isTrue();
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testWrittenDataSize()
    {
        skipTestUnless(hasBehavior(SUPPORTS_REPORTING_WRITTEN_BYTES));
        String tableName = "write_stats_" + randomNameSuffix();
        try {
            String query = "CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation";
            assertQueryStats(
                    getSession(),
                    query,
                    queryStats -> assertThat(queryStats.getPhysicalWrittenDataSize().toBytes()).isPositive(),
                    results -> {});
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    /**
     * Some connectors support system table denoted with $-suffix. Ensure no connector exposes table_name$data
     * directly to users, as it would mean the same thing as table_name itself.
     */
    @Test
    public void testNoDataSystemTable()
    {
        assertQuerySucceeds("TABLE nation");
        assertQueryFails("TABLE \"nation$data\"", "line 1:1: Table '\\w+.\\w+.\"nation\\$data\"' does not exist");
    }

    @Test
    public void testColumnName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        for (String columnName : testColumnNameDataProvider()) {
            testColumnName(columnName, requiresDelimiting(columnName));
        }
    }

    protected void testColumnName(String columnName, boolean delimited)
    {
        String nameInSql = toColumnNameInSql(columnName, delimited);
        String tableName = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "") + randomNameSuffix();

        try {
            // TODO test with both CTAS *and* CREATE TABLE + INSERT, since they use different connector API methods.
            assertUpdate("CREATE TABLE " + tableName + "(key varchar(50), " + nameInSql + " varchar(50))");
        }
        catch (RuntimeException e) {
            if (isColumnNameRejected(e, columnName, delimited)) {
                // It is OK if give column name is not allowed and is clearly rejected by the connector.
                return;
            }
            throw e;
        }
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES ('null value', NULL), ('sample value', 'abc'), ('other value', 'xyz')", 3);

            // SELECT *
            assertQuery("SELECT * FROM " + tableName, "VALUES ('null value', NULL), ('sample value', 'abc'), ('other value', 'xyz')");

            // projection
            assertQuery("SELECT " + nameInSql + " FROM " + tableName, "VALUES (NULL), ('abc'), ('xyz')");

            // predicate
            assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " IS NULL", "VALUES ('null value')");
            assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " = 'abc'", "VALUES ('sample value')");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testAddAndDropColumnName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_ADD_COLUMN) && hasBehavior(SUPPORTS_DROP_COLUMN));

        for (String columnName : testColumnNameDataProvider()) {
            testAddAndDropColumnName(columnName, requiresDelimiting(columnName));
        }
    }

    protected void testAddAndDropColumnName(String columnName, boolean delimited)
    {
        String nameInSql = toColumnNameInSql(columnName, delimited);
        String tableName = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "") + randomNameSuffix();

        try {
            assertUpdate(createTableSqlForAddingAndDroppingColumn(tableName, nameInSql));
        }
        catch (RuntimeException e) {
            if (isColumnNameRejected(e, columnName, delimited)) {
                // It is OK if given column name is not allowed and is clearly rejected by the connector.
                return;
            }
            throw e;
        }
        assertTableColumnNames(tableName, columnName.toLowerCase(ENGLISH), "value");

        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN " + nameInSql);
        assertTableColumnNames(tableName, "value");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN " + nameInSql + " varchar(50)");
        assertTableColumnNames(tableName, "value", columnName.toLowerCase(ENGLISH));

        assertUpdate("DROP TABLE " + tableName);
    }

    /**
     * Create a table with name "tableName" and with two columns: "columnNameInSql" varchar(50), value varchar(50)
     */
    protected String createTableSqlForAddingAndDroppingColumn(String tableName, String columnNameInSql)
    {
        return "CREATE TABLE " + tableName + "(" + columnNameInSql + " varchar(50), value varchar(50))";
    }

    @Test
    public void testRenameColumnName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_RENAME_COLUMN));

        for (String columnName : testColumnNameDataProvider()) {
            testRenameColumnName(columnName, requiresDelimiting(columnName));
        }
    }

    protected void testRenameColumnName(String columnName, boolean delimited)
    {
        String nameInSql = toColumnNameInSql(columnName, delimited);
        String tableName = "tcn_" + nameInSql.replaceAll("[^a-z0-9]", "") + randomNameSuffix();
        // Use complex identifier to test a source column name when renaming columns
        String sourceColumnName = "a;b$c";

        try {
            assertUpdate("CREATE TABLE " + tableName + "(\"" + sourceColumnName + "\" varchar(50))");
            assertTableColumnNames(tableName, sourceColumnName);

            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN \"" + sourceColumnName + "\" TO " + nameInSql);
            assertTableColumnNames(tableName, columnName.toLowerCase(ENGLISH));
        }
        catch (RuntimeException e) {
            if (isColumnNameRejected(e, columnName, delimited)) {
                // It is OK if give column name is not allowed and is clearly rejected by the connector.
                return;
            }
            throw e;
        }

        assertUpdate("DROP TABLE " + tableName);
    }

    private static String toColumnNameInSql(String columnName, boolean delimited)
    {
        String nameInSql = columnName;
        if (delimited) {
            nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
        }
        return nameInSql;
    }

    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return false;
    }

    protected static boolean requiresDelimiting(String identifierName)
    {
        return !identifierName.matches("[a-zA-Z][a-zA-Z0-9_]*");
    }

    public List<String> testColumnNameDataProvider()
    {
        return testColumnNameTestData().stream()
                .map(this::filterColumnNameTestData)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());
    }

    private List<String> testColumnNameTestData()
    {
        return ImmutableList.<String>builder()
                .add("lowercase")
                .add("UPPERCASE")
                .add("MixedCase")
                .add("an_underscore")
                .add("a-hyphen-minus") // ASCII '-' is HYPHEN-MINUS in Unicode
                .add("a space")
                .add("atrailingspace ")
                .add(" aleadingspace")
                .add("a.dot")
                .add("a,comma")
                .add("a:colon")
                .add("a;semicolon")
                .add("an@at")
                .add("a\"quote")
                .add("an'apostrophe")
                .add("a`backtick`")
                .add("a/slash`")
                .add("a\\backslash`")
                .add("adigit0")
                .add("0startwithdigit")
                .add("カラム")
                .build();
    }

    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        return Optional.of(columnName);
    }

    protected String dataMappingTableName(String trinoTypeName)
    {
        return "test_data_mapping_smoke_" + trinoTypeName.replaceAll("[^a-zA-Z0-9]", "_") + randomNameSuffix();
    }

    @Test
    public void testCreateTableWithTableCommentSpecialCharacter()
    {
        testCreateTableWithTableCommentSpecialCharacter("a;semicolon");
        testCreateTableWithTableCommentSpecialCharacter("an@at");
        testCreateTableWithTableCommentSpecialCharacter("a\"quote");
        testCreateTableWithTableCommentSpecialCharacter("an'apostrophe");
        testCreateTableWithTableCommentSpecialCharacter("a`backtick`");
        testCreateTableWithTableCommentSpecialCharacter("a/slash");
        testCreateTableWithTableCommentSpecialCharacter("a\\backslash");
        testCreateTableWithTableCommentSpecialCharacter("a?question");
        testCreateTableWithTableCommentSpecialCharacter("[square bracket]");
    }

    protected void testCreateTableWithTableCommentSpecialCharacter(String comment)
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_", "(a bigint) COMMENT " + varcharLiteral(comment))) {
            assertThat(getTableComment(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), table.getName())).isEqualTo(comment);
        }
    }

    @Test
    public void testCreateTableAsSelectWithTableCommentSpecialCharacter()
    {
        testCreateTableAsSelectWithTableCommentSpecialCharacter("a;semicolon");
        testCreateTableAsSelectWithTableCommentSpecialCharacter("an@at");
        testCreateTableAsSelectWithTableCommentSpecialCharacter("a\"quote");
        testCreateTableAsSelectWithTableCommentSpecialCharacter("an'apostrophe");
        testCreateTableAsSelectWithTableCommentSpecialCharacter("a`backtick`");
        testCreateTableAsSelectWithTableCommentSpecialCharacter("a/slash");
        testCreateTableAsSelectWithTableCommentSpecialCharacter("a\\backslash");
        testCreateTableAsSelectWithTableCommentSpecialCharacter("a?question");
        testCreateTableAsSelectWithTableCommentSpecialCharacter("[square bracket]");
    }

    private void testCreateTableAsSelectWithTableCommentSpecialCharacter(String comment)
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_", " COMMENT " + varcharLiteral(comment) + " AS SELECT 1 a")) {
            assertThat(getTableComment(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), table.getName())).isEqualTo(comment);
        }
    }

    @Test
    public void testCreateTableWithColumnCommentSpecialCharacter()
    {
        testCreateTableWithColumnCommentSpecialCharacter("a;semicolon");
        testCreateTableWithColumnCommentSpecialCharacter("an@at");
        testCreateTableWithColumnCommentSpecialCharacter("a\"quote");
        testCreateTableWithColumnCommentSpecialCharacter("an'apostrophe");
        testCreateTableWithColumnCommentSpecialCharacter("a`backtick`");
        testCreateTableWithColumnCommentSpecialCharacter("a/slash");
        testCreateTableWithColumnCommentSpecialCharacter("a\\backslash");
        testCreateTableWithColumnCommentSpecialCharacter("a?question");
        testCreateTableWithColumnCommentSpecialCharacter("[square bracket]");
    }

    private void testCreateTableWithColumnCommentSpecialCharacter(String comment)
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_COLUMN_COMMENT));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_create_", " (a bigint COMMENT " + varcharLiteral(comment) + ")")) {
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo(comment);
        }
    }

    @Test
    public void testAddColumnWithCommentSpecialCharacter()
    {
        testAddColumnWithCommentSpecialCharacter("a;semicolon");
        testAddColumnWithCommentSpecialCharacter("an@at");
        testAddColumnWithCommentSpecialCharacter("a\"quote");
        testAddColumnWithCommentSpecialCharacter("an'apostrophe");
        testAddColumnWithCommentSpecialCharacter("a`backtick`");
        testAddColumnWithCommentSpecialCharacter("a/slash");
        testAddColumnWithCommentSpecialCharacter("a\\backslash");
        testAddColumnWithCommentSpecialCharacter("a?question");
        testAddColumnWithCommentSpecialCharacter("[square bracket]");
    }

    protected void testAddColumnWithCommentSpecialCharacter(String comment)
    {
        skipTestUnless(hasBehavior(SUPPORTS_ADD_COLUMN_WITH_COMMENT));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_col_", "(a_varchar varchar)")) {
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN b_varchar varchar COMMENT " + varcharLiteral(comment));
            assertThat(getColumnComment(table.getName(), "b_varchar")).isEqualTo(comment);
        }
    }

    @Test
    public void testCommentTableSpecialCharacter()
    {
        testCommentTableSpecialCharacter("a;semicolon");
        testCommentTableSpecialCharacter("an@at");
        testCommentTableSpecialCharacter("a\"quote");
        testCommentTableSpecialCharacter("an'apostrophe");
        testCommentTableSpecialCharacter("a`backtick`");
        testCommentTableSpecialCharacter("a/slash");
        testCommentTableSpecialCharacter("a\\backslash");
        testCommentTableSpecialCharacter("a?question");
        testCommentTableSpecialCharacter("[square bracket]");
    }

    private void testCommentTableSpecialCharacter(String comment)
    {
        skipTestUnless(hasBehavior(SUPPORTS_COMMENT_ON_TABLE));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_table_", "(a integer)")) {
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS " + varcharLiteral(comment));
            assertThat(getTableComment(getSession().getCatalog().orElseThrow(), getSession().getSchema().orElseThrow(), table.getName())).isEqualTo(comment);
        }
    }

    @Test
    public void testCommentColumnSpecialCharacter()
    {
        testCommentColumnSpecialCharacter("a;semicolon");
        testCommentColumnSpecialCharacter("an@at");
        testCommentColumnSpecialCharacter("a\"quote");
        testCommentColumnSpecialCharacter("an'apostrophe");
        testCommentColumnSpecialCharacter("a`backtick`");
        testCommentColumnSpecialCharacter("a/slash");
        testCommentColumnSpecialCharacter("a\\backslash");
        testCommentColumnSpecialCharacter("a?question");
        testCommentColumnSpecialCharacter("[square bracket]");
    }

    private void testCommentColumnSpecialCharacter(String comment)
    {
        skipTestUnless(hasBehavior(SUPPORTS_COMMENT_ON_COLUMN));

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_column_", "(a integer)")) {
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS " + varcharLiteral(comment));
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo(comment);
        }
    }

    protected static String varcharLiteral(String value)
    {
        requireNonNull(value, "value is null");
        return "'" + value.replace("'", "''") + "'";
    }

    @Test
    public void testDataMappingSmokeTest()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        for (DataMappingTestSetup dataMappingTestSetup : testDataMappingSmokeTestDataProvider()) {
            testDataMapping(dataMappingTestSetup);
        }
    }

    private void testDataMapping(DataMappingTestSetup dataMappingTestSetup)
    {
        log.info("dataMappingTestSetup=%s", dataMappingTestSetup);
        String trinoTypeName = dataMappingTestSetup.getTrinoTypeName();
        String sampleValueLiteral = dataMappingTestSetup.getSampleValueLiteral();
        String highValueLiteral = dataMappingTestSetup.getHighValueLiteral();

        String tableName = dataMappingTableName(trinoTypeName);

        Runnable setup = () -> {
            // TODO test with both CTAS *and* CREATE TABLE + INSERT, since they use different connector API methods.
            String createTable = "" +
                    "CREATE TABLE " + tableName + " AS " +
                    "SELECT CAST(row_id AS varchar(50)) row_id, CAST(value AS " + trinoTypeName + ") value, CAST(value AS " + trinoTypeName + ") another_column " +
                    "FROM (VALUES " +
                    "  ('null value', NULL), " +
                    "  ('sample value', " + sampleValueLiteral + "), " +
                    "  ('high value', " + highValueLiteral + ")) " +
                    " t(row_id, value)";
            assertUpdate(createTable, 3);
        };
        if (dataMappingTestSetup.isUnsupportedType()) {
            assertThatThrownBy(setup::run)
                    .satisfies(exception -> verifyUnsupportedTypeException(exception, trinoTypeName));
            return;
        }
        setup.run();

        // without pushdown, i.e. test read data mapping
        assertQuery("SELECT row_id FROM " + tableName + " WHERE rand() = 42 OR value IS NULL", "VALUES 'null value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE rand() = 42 OR value IS NOT NULL", "VALUES 'sample value', 'high value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE rand() = 42 OR value = " + sampleValueLiteral, "VALUES 'sample value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE rand() = 42 OR value = " + highValueLiteral, "VALUES 'high value'");

        assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL", "VALUES 'null value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NOT NULL", "VALUES 'sample value', 'high value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value = " + sampleValueLiteral, "VALUES 'sample value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value != " + sampleValueLiteral, "VALUES 'high value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value <= " + sampleValueLiteral, "VALUES 'sample value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value > " + sampleValueLiteral, "VALUES 'high value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value <= " + highValueLiteral, "VALUES 'sample value', 'high value'");

        assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value = " + sampleValueLiteral, "VALUES 'null value', 'sample value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value != " + sampleValueLiteral, "VALUES 'null value', 'high value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value <= " + sampleValueLiteral, "VALUES 'null value', 'sample value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value > " + sampleValueLiteral, "VALUES 'null value', 'high value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value <= " + highValueLiteral, "VALUES 'null value', 'sample value', 'high value'");

        // complex condition, one that cannot be represented with a TupleDomain
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value = " + sampleValueLiteral + " OR another_column = " + sampleValueLiteral, "VALUES 'sample value'");

        assertUpdate("DROP TABLE " + tableName);
    }

    public final List<DataMappingTestSetup> testDataMappingSmokeTestDataProvider()
    {
        return testDataMappingSmokeTestData().stream()
                .map(this::filterDataMappingSmokeTestData)
                .flatMap(Optional::stream)
                .collect(toList());
    }

    private List<DataMappingTestSetup> testDataMappingSmokeTestData()
    {
        return ImmutableList.<DataMappingTestSetup>builder()
                .add(new DataMappingTestSetup("boolean", "false", "true"))
                .add(new DataMappingTestSetup("tinyint", "37", "127"))
                .add(new DataMappingTestSetup("smallint", "32123", "32767"))
                .add(new DataMappingTestSetup("integer", "1274942432", "2147483647"))
                .add(new DataMappingTestSetup("bigint", "312739231274942432", "9223372036854775807"))
                .add(new DataMappingTestSetup("real", "REAL '567.123'", "REAL '999999.999'"))
                .add(new DataMappingTestSetup("double", "DOUBLE '1234567890123.123'", "DOUBLE '9999999999999.999'"))
                .add(new DataMappingTestSetup("decimal(5,3)", "12.345", "99.999"))
                .add(new DataMappingTestSetup("decimal(15,3)", "123456789012.345", "999999999999.99"))
                .add(new DataMappingTestSetup("date", "DATE '0001-01-01'", "DATE '1582-10-04'")) // before julian->gregorian switch
                .add(new DataMappingTestSetup("date", "DATE '1582-10-05'", "DATE '1582-10-14'")) // during julian->gregorian switch
                .add(new DataMappingTestSetup("date", "DATE '2020-02-12'", "DATE '9999-12-31'"))
                .add(new DataMappingTestSetup("time", "TIME '15:03:00'", "TIME '23:59:59.999'"))
                .add(new DataMappingTestSetup("time(6)", "TIME '15:03:00'", "TIME '23:59:59.999999'"))
                .add(new DataMappingTestSetup("timestamp", "TIMESTAMP '1969-12-31 15:03:00.123'", "TIMESTAMP '1969-12-31 17:03:00.456'"))
                .add(new DataMappingTestSetup("timestamp", "TIMESTAMP '2020-02-12 15:03:00'", "TIMESTAMP '2199-12-31 23:59:59.999'"))
                .add(new DataMappingTestSetup("timestamp(6)", "TIMESTAMP '1969-12-31 15:03:00.123456'", "TIMESTAMP '1969-12-31 17:03:00.123456'"))
                .add(new DataMappingTestSetup("timestamp(6)", "TIMESTAMP '2020-02-12 15:03:00'", "TIMESTAMP '2199-12-31 23:59:59.999999'"))
                .add(new DataMappingTestSetup("timestamp(3) with time zone", "TIMESTAMP '1969-12-31 15:03:00.123 +01:00'", "TIMESTAMP '1969-12-31 17:03:00.456 +01:00'"))
                .add(new DataMappingTestSetup("timestamp(3) with time zone", "TIMESTAMP '2020-02-12 15:03:00 +01:00'", "TIMESTAMP '9999-12-31 23:59:59.999 +12:00'"))
                .add(new DataMappingTestSetup("timestamp(6) with time zone", "TIMESTAMP '1969-12-31 15:03:00.123456 +01:00'", "TIMESTAMP '1969-12-31 17:03:00.123456 +01:00'"))
                .add(new DataMappingTestSetup("timestamp(6) with time zone", "TIMESTAMP '2020-02-12 15:03:00 +01:00'", "TIMESTAMP '9999-12-31 23:59:59.999999 +12:00'"))
                .add(new DataMappingTestSetup("char(3)", "'ab'", "'zzz'"))
                .add(new DataMappingTestSetup("varchar(3)", "'de'", "'zzz'"))
                .add(new DataMappingTestSetup("varchar", "'łąka for the win'", "'ŻŻŻŻŻŻŻŻŻŻ'"))
                .add(new DataMappingTestSetup("varchar", "'a \\backslash'", "'a a'")) // `a` sorts after `\`; \b may be interpreted as an escape sequence
                .add(new DataMappingTestSetup("varchar", "'end backslash \\'", "'end backslash a'")) // `a` sorts after `\`; final \ before end quote may confuse a parser
                .add(new DataMappingTestSetup("varchar", "U&'a \\000a newline'", "'a a'")) // `a` sorts after `\n`; newlines can require special handling in a remote system's language
                .add(new DataMappingTestSetup("varbinary", "X'12ab3f'", "X'ffffffffffffffffffff'"))
                .build();
    }

    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        return Optional.of(dataMappingTestSetup);
    }

    @Test
    public void testCaseSensitiveDataMapping()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));

        for (DataMappingTestSetup dataMappingTestSetup : testCaseSensitiveDataMappingProvider()) {
            testDataMapping(dataMappingTestSetup);
        }
    }

    private List<DataMappingTestSetup> testCaseSensitiveDataMappingProvider()
    {
        return testCaseSensitiveDataMappingData().stream()
                .map(this::filterCaseSensitiveDataMappingTestData)
                .flatMap(Optional::stream)
                .collect(toList());
    }

    protected Optional<DataMappingTestSetup> filterCaseSensitiveDataMappingTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        return Optional.of(dataMappingTestSetup);
    }

    private List<DataMappingTestSetup> testCaseSensitiveDataMappingData()
    {
        return ImmutableList.<DataMappingTestSetup>builder()
                .add(new DataMappingTestSetup("char(1)", "'A'", "'a'"))
                .add(new DataMappingTestSetup("varchar(1)", "'A'", "'a'"))
                .add(new DataMappingTestSetup("char(1)", "'A'", "'b'"))
                .add(new DataMappingTestSetup("varchar(1)", "'A'", "'b'"))
                .add(new DataMappingTestSetup("char(1)", "'B'", "'a'"))
                .add(new DataMappingTestSetup("varchar(1)", "'B'", "'a'"))
                .build();
    }

    @Test
    public void testTimestampWithTimeZoneCastToDatePredicate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        TestTable table;
        try {
            table = new TestTable(
                    getQueryRunner()::execute,
                    "timestamptz_to_date",
                    // These to timestamps are same local time, but different point in times and also different date at UTC time zone
                    """
                    (i, t) AS VALUES
                        ('UTC', TIMESTAMP '2005-09-10 00:12:34.000 UTC'),
                        ('Warsaw', TIMESTAMP '2005-09-10 00:12:34.000 Europe/Warsaw'),
                        ('Los Angeles', TIMESTAMP '2005-09-10 00:12:34.000 America/Los_Angeles')
                    """);
        }
        catch (QueryFailedException e) {
            verifyUnsupportedTypeException(e, "timestamp(3) with time zone");
            return;
        }
        try (table) {
            assertThat(query("SELECT i FROM " + table.getName() + " WHERE CAST(t AS date) = DATE '2005-09-10'"))
                    .hasCorrectResultsRegardlessOfPushdown()
                    // Number of matched rows depends on whether the connector preserves the time zone information, or point in time only
                    .skippingTypesCheck()
                    .containsAll("VALUES 'UTC', 'Los Angeles'");
        }
    }

    @Test
    public void testTimestampWithTimeZoneCastToTimestampPredicate()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        TestTable table;
        try {
            table = new TestTable(
                    getQueryRunner()::execute,
                    "timestamptz_to_ts",
                    // These to timestamps are same local time, but different point in times
                    """
                    (i, t) AS VALUES
                        ('UTC', TIMESTAMP '2005-09-10 13:00:00.000 UTC'),
                        ('Warsaw', TIMESTAMP '2005-09-10 13:00:00.000 Europe/Warsaw'),
                        ('Los Angeles', TIMESTAMP '2005-09-10 13:00:00.000 America/Los_Angeles')
                    """);
        }
        catch (QueryFailedException e) {
            verifyUnsupportedTypeException(e, "timestamp(3) with time zone");
            return;
        }
        try (table) {
            assertThat(query("SELECT i FROM " + table.getName() + " WHERE CAST(t AS timestamp(0)) = TIMESTAMP '2005-09-10 13:00:00'"))
                    .hasCorrectResultsRegardlessOfPushdown()
                    // Number of matched rows depends on whether the connector preserves the time zone information, or point in time only
                    .skippingTypesCheck()
                    .containsAll("VALUES 'UTC'");
        }
    }

    /**
     * A regression test for row (struct) dereference pushdown edge case, with duplicate expressions.
     * See https://github.com/trinodb/trino/issues/11559 and https://github.com/trinodb/trino/issues/11560.
     */
    @Test
    public void testPotentialDuplicateDereferencePushdown()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        String tableName = "test_dup_deref_" + randomNameSuffix();
        String createTable = "CREATE TABLE " + tableName + " AS SELECT CAST(ROW('abc', 1) AS row(a varchar, b bigint)) r";
        if (!hasBehavior(SUPPORTS_ROW_TYPE)) {
            try {
                assertUpdate(createTable);
            }
            catch (Exception expected) {
                verifyUnsupportedTypeException(expected, "row(a varchar, b bigint)");
                return;
            }
            assertUpdate("DROP TABLE " + tableName);
            fail("Expected create table failure");
        }

        assertUpdate(createTable, 1);
        try {
            assertThat(query("SELECT r, r.b + 2 FROM " + tableName))
                    .matches("SELECT CAST(ROW('abc', 1) AS ROW(a varchar, b bigint)), BIGINT '3'");

            assertThat(query("SELECT r[1], r[2], r.b + 2 FROM " + tableName))
                    .matches("VALUES (VARCHAR 'abc', BIGINT '1', BIGINT '3')");

            assertThat(query("SELECT r[2], r.b + 2 FROM " + tableName))
                    .matches("VALUES (BIGINT '1', BIGINT '3')");

            assertThat(query("SELECT r.b, r.b + 2 FROM " + tableName))
                    .matches("VALUES (BIGINT '1', BIGINT '3')");

            assertThat(query("SELECT r, r.a LIKE '%c' FROM " + tableName))
                    .matches("SELECT CAST(ROW('abc', 1) AS ROW(a varchar, b bigint)), true");

            assertThat(query("SELECT r[1], r[2], r.a LIKE '%c' FROM " + tableName))
                    .matches("VALUES (VARCHAR 'abc', BIGINT '1', true)");

            assertThat(query("SELECT r[1], r.a LIKE '%c' FROM " + tableName))
                    .matches("VALUES (VARCHAR 'abc', true)");

            assertThat(query("SELECT r.a, r.a LIKE '%c' FROM " + tableName))
                    .matches("VALUES (VARCHAR 'abc', true)");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    public void testMergeDeleteWithCTAS()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE) && hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA));

        String target = "merge_target_with_ctas_" + randomNameSuffix();
        String source = "merge_source_with_ctas_" + randomNameSuffix();
        @Language("SQL") String createTableSql =
                """
                CREATE TABLE %s AS
                SELECT * FROM (
                        VALUES
                        (1, 'a', 'aa'),
                        (2, 'b', 'bb'),
                        (3, 'c', 'cc'),
                        (4, 'd', 'dd')
                ) AS t (id, name, value)
                """;
        assertUpdate(createTableSql.formatted(target), 4);
        assertUpdate(createTableSql.formatted(source), 4);

        assertQuery("SELECT COUNT(*) FROM " + target, "VALUES 4");
        assertUpdate("DELETE FROM %s WHERE id IN (SELECT id FROM %s WHERE id > 2)".formatted(target, source), 2);
        assertQuery("SELECT * FROM " + target, "VALUES (1, 'a', 'aa'), (2, 'b', 'bb')");
        assertUpdate("MERGE INTO %s t USING %s s ON (t.id = s.id) WHEN MATCHED AND s.id > 1 THEN DELETE".formatted(target, source), 1);
        assertQuery("SELECT * FROM " + target, "VALUES (1, 'a', 'aa')");

        assertUpdate("DROP TABLE " + target);
        assertUpdate("DROP TABLE " + source);
    }

    protected String createTableForWrites(String createTable)
    {
        return createTable;
    }

    @Test
    public void testMergeLarge()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE) && hasBehavior(SUPPORTS_INSERT));

        String tableName = "test_merge_" + randomNameSuffix();

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (orderkey BIGINT, custkey BIGINT, totalprice DOUBLE)", tableName)));

        assertUpdate(
                format("INSERT INTO %s SELECT orderkey, custkey, totalprice FROM tpch.sf1.orders", tableName),
                (long) computeScalar("SELECT count(*) FROM tpch.sf1.orders"));

        @Language("SQL") String mergeSql = "" +
                "MERGE INTO " + tableName + " t USING (SELECT * FROM tpch.sf1.orders) s ON (t.orderkey = s.orderkey)\n" +
                "WHEN MATCHED AND mod(s.orderkey, 3) = 0 THEN UPDATE SET totalprice = t.totalprice + s.totalprice\n" +
                "WHEN MATCHED AND mod(s.orderkey, 3) = 1 THEN DELETE";

        assertUpdate(mergeSql, 1_000_000);

        // verify deleted rows
        assertQuery("SELECT count(*) FROM " + tableName + " WHERE mod(orderkey, 3) = 1", "SELECT 0");

        // verify untouched rows
        assertThat(query("SELECT count(*), sum(cast(totalprice AS decimal(18,2))) FROM " + tableName + " WHERE mod(orderkey, 3) = 2"))
                .matches("SELECT count(*), sum(cast(totalprice AS decimal(18,2))) FROM tpch.sf1.orders WHERE mod(orderkey, 3) = 2");

        // verify updated rows
        assertThat(query("SELECT count(*), sum(cast(totalprice AS decimal(18,2))) FROM " + tableName + " WHERE mod(orderkey, 3) = 0"))
                .matches("SELECT count(*), sum(cast(totalprice AS decimal(18,2)) * 2) FROM tpch.sf1.orders WHERE mod(orderkey, 3) = 0");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testMergeSimpleSelect()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_simple_target_" + randomNameSuffix();
        String sourceTable = "merge_simple_source_" + randomNameSuffix();
        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

        assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)", 4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Ed', 7, 'Etherville'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeFruits()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_various_target_" + randomNameSuffix();
        String sourceTable = "merge_various_source_" + randomNameSuffix();
        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR)", targetTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles')", targetTable), 3);

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR)", sourceTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Craig', 'candles'), ('Len', 'limes'), ('Joe', 'jellybeans')", sourceTable), 3);

        assertUpdate(format("MERGE INTO %s t USING %s s ON (t.purchase = s.purchase)", targetTable, sourceTable) +
                "    WHEN MATCHED AND s.purchase = 'limes' THEN DELETE" +
                "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_', s.customer)" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchase) VALUES(s.customer, s.purchase)", 3);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Dave', 'dates'), ('Carol_Craig', 'candles'), ('Joe', 'jellybeans')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeMultipleOperations()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        int targetCustomerCount = 32;
        String targetTable = "merge_multiple_" + randomNameSuffix();
        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, zipcode INT, spouse VARCHAR, address VARCHAR)", targetTable)));

        String originalInsertFirstHalf = IntStream.range(1, targetCustomerCount / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 1000, 91000, intValue, intValue))
                .collect(Collectors.joining(", "));
        String originalInsertSecondHalf = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 2000, 92000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s, %s", targetTable, originalInsertFirstHalf, originalInsertSecondHalf), targetCustomerCount - 1);

        String firstMergeSource = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 83000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("MERGE INTO %s t USING (VALUES %s) AS s(customer, purchases, zipcode, spouse, address)", targetTable, firstMergeSource) +
                        "    ON t.customer = s.customer" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases, zipcode = s.zipcode, spouse = s.spouse, address = s.address",
                targetCustomerCount / 2);

        assertQuery(
                "SELECT customer, purchases, zipcode, spouse, address FROM " + targetTable,
                format("VALUES %s, %s", originalInsertFirstHalf, firstMergeSource));

        String nextInsert = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s", targetTable, nextInsert), targetCustomerCount / 2);

        String secondMergeSource = IntStream.range(1, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertUpdate(format("MERGE INTO %s t USING (VALUES %s) AS s(customer, purchases, zipcode, spouse, address)", targetTable, secondMergeSource) +
                        "    ON t.customer = s.customer" +
                        "    WHEN MATCHED AND t.zipcode = 91000 THEN DELETE" +
                        "    WHEN MATCHED AND s.zipcode = 85000 THEN UPDATE SET zipcode = 60000" +
                        "    WHEN MATCHED THEN UPDATE SET zipcode = s.zipcode, spouse = s.spouse, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, zipcode, spouse, address) VALUES(s.customer, s.purchases, s.zipcode, s.spouse, s.address)",
                targetCustomerCount * 3 / 2 - 1);

        String updatedBeginning = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 60000, intValue, intValue))
                .collect(Collectors.joining(", "));
        String updatedMiddle = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(Collectors.joining(", "));
        String updatedEnd = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                .collect(Collectors.joining(", "));

        assertQuery(
                "SELECT customer, purchases, zipcode, spouse, address FROM " + targetTable,
                format("VALUES %s, %s, %s", updatedBeginning, updatedMiddle, updatedEnd));

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeSimpleQuery()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_query_" + randomNameSuffix();
        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        assertUpdate(format("MERGE INTO %s t USING ", targetTable) +
                        "(VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville')) AS s(customer, purchases, address)" +
                        " ON (t.customer = s.customer)" +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)",
                4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeAllInserts()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_inserts_" + randomNameSuffix();
        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena')", targetTable), 2);

        assertUpdate(format("MERGE INTO %s t USING ", targetTable) +
                        "(VALUES ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire')) AS s(customer, purchases, address)" +
                        " ON (t.customer = s.customer)" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)",
                2);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeFalseJoinCondition()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_join_false_" + randomNameSuffix();
        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena')", targetTable), 2);

        // Test a literal false
        assertUpdate(
                """
                MERGE INTO %s t USING (VALUES ('Carol', 9, 'Centreville')) AS s(customer, purchases, address)
                  ON (FALSE)
                    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)
                """.formatted(targetTable),
                1);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 9, 'Centreville')");

        // Test a constant-folded false expression
        assertUpdate(
                """
                MERGE INTO %s t USING (VALUES ('Dave', 22, 'Darbyshire')) AS s(customer, purchases, address)
                  ON (t.customer != t.customer)
                    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)
                """.formatted(targetTable),
                1);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire')");

        // Test a more complicated constant-folded false expression
        assertUpdate(
                """
                MERGE INTO %s t USING (VALUES ('Ed', 7, 'Etherville')) AS s(customer, purchases, address)
                  ON (23 - (12 + 10) > 1)
                    WHEN MATCHED THEN UPDATE SET customer = concat(s.customer, '_fooled_you')
                    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)
                """.formatted(targetTable),
                1);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeAllColumnsUpdated()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_all_columns_updated_target_" + randomNameSuffix();
        String sourceTable = "merge_all_columns_updated_source_" + randomNameSuffix();
        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Devon'), ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge')", targetTable), 4);

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Darbyshire'), ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Ed', 7, 'Etherville')", sourceTable), 4);

        assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_updated'), purchases = s.purchases + t.purchases, address = s.address",
                3);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Dave_updated', 22, 'Darbyshire'), ('Aaron_updated', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Carol_updated', 12, 'Centreville')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeAllMatchesDeleted()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_all_matches_deleted_target_" + randomNameSuffix();
        String sourceTable = "merge_all_matches_deleted_source_" + randomNameSuffix();
        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville')", sourceTable), 4);

        assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED THEN DELETE",
                3);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Bill', 7, 'Buena')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeMultipleRowsMatchFails()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_multiple_fail_target_" + randomNameSuffix();
        String sourceTable = "merge_multiple_fail_source_" + randomNameSuffix();

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Antioch')", targetTable), 2);

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (id INT, customer VARCHAR, purchases INT, address VARCHAR)", sourceTable)));

        assertUpdate(format("INSERT INTO %s (id, customer, purchases, address) VALUES (1, 'Aaron', 6, 'Adelphi'), (2, 'Aaron', 8, 'Ashland')", sourceTable), 2);

        assertQueryFails(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED THEN UPDATE SET address = s.address",
                "One MERGE target table row matched more than one source row");

        assertUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Adelphi' THEN UPDATE SET address = s.address",
                1);
        assertQuery("SELECT customer, purchases, address FROM " + targetTable, "VALUES ('Aaron', 5, 'Adelphi'), ('Bill', 7, 'Antioch')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeQueryWithStrangeCapitalization()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_strange_capitalization_" + randomNameSuffix();
        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        assertUpdate(format("MERGE INTO %s t USING ", targetTable.toUpperCase(ENGLISH)) +
                        "(VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville')) AS s(customer, purchases, address)" +
                        "ON (t.customer = s.customer)" +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purCHases = s.PurchaseS + t.pUrchases, aDDress = s.addrESs" +
                        "    WHEN NOT MATCHED THEN INSERT (CUSTOMER, purchases, addRESS) VALUES(s.custoMer, s.Purchases, s.ADDress)",
                4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeWithoutTablesAliases()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "test_without_aliases_target_" + randomNameSuffix();
        String sourceTable = "test_without_aliases_source_" + randomNameSuffix();
        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", targetTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable), 4);

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable), 4);

        assertUpdate(format("MERGE INTO %s USING %s", targetTable, sourceTable) +
                        format(" ON (%s.customer = %s.customer)", targetTable, sourceTable) +
                        format("    WHEN MATCHED AND %s.address = 'Centreville' THEN DELETE", sourceTable) +
                        format("    WHEN MATCHED THEN UPDATE SET purchases = %s.pURCHases + %s.pUrchases, aDDress = %s.addrESs", sourceTable, targetTable, sourceTable) +
                        format("    WHEN NOT MATCHED THEN INSERT (cusTomer, purchases, addRESS) VALUES(%s.custoMer, %s.Purchases, %s.ADDress)", sourceTable, sourceTable, sourceTable),
                4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('Aaron', 11, 'Arches'), ('Bill', 7, 'Buena'), ('Dave', 22, 'Darbyshire'), ('Ed', 7, 'Etherville')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeWithUnpredictablePredicates()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_predicates_target_" + randomNameSuffix();
        String sourceTable = "merge_predicates_source_" + randomNameSuffix();

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (id INT, customer VARCHAR, purchases INT, address VARCHAR)", targetTable)));

        assertUpdate(format("INSERT INTO %s (id, customer, purchases, address) VALUES (1, 'Aaron', 5, 'Antioch'), (2, 'Bill', 7, 'Buena'), (3, 'Carol', 3, 'Cambridge'), (4, 'Dave', 11, 'Devon')", targetTable), 4);

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (id INT, customer VARCHAR, purchases INT, address VARCHAR)", sourceTable)));

        assertUpdate(format("INSERT INTO %s (id, customer, purchases, address) VALUES (5, 'Aaron', 6, 'Arches'), (6, 'Carol', 9, 'Centreville'), (7, 'Dave', 11, 'Darbyshire'), (8, 'Ed', 7, 'Etherville')", sourceTable), 4);

        assertUpdate(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        " ON t.customer = s.customer AND s.purchases < 10.2" +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (id, customer, purchases, address) VALUES (s.id, s.customer, s.purchases, s.address)",
                4);

        assertQuery("SELECT * FROM " + targetTable, "VALUES (1, 'Aaron', 11, 'Arches'), (2, 'Bill', 7, 'Buena'), (7, 'Dave', 11, 'Darbyshire'), (4, 'Dave', 11, 'Devon'), (8, 'Ed', 7, 'Etherville')");

        assertUpdate(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        " ON t.customer = s.customer" +
                        "    WHEN MATCHED AND t.address <> 'Darbyshire' AND s.purchases * 2 > 20" +
                        "        THEN DELETE" +
                        "    WHEN MATCHED" +
                        "        THEN UPDATE SET purchases = s.purchases + t.purchases, address = concat(t.address, '/', s.address)" +
                        "    WHEN NOT MATCHED" +
                        "        THEN INSERT (id, customer, purchases, address) VALUES (s.id, s.customer, s.purchases, s.address)",
                5);

        assertQuery(
                "SELECT * FROM " + targetTable,
                "VALUES (1, 'Aaron', 17, 'Arches/Arches'), (2, 'Bill', 7, 'Buena'), (6, 'Carol', 9, 'Centreville'), (7, 'Dave', 22, 'Darbyshire/Darbyshire'), (8, 'Ed', 14, 'Etherville/Etherville')");

        assertUpdate(format("INSERT INTO %s (id, customer, purchases, address) VALUES (9, 'Fred', 30, 'Franklin')", targetTable), 1);
        assertQuery(
                "SELECT * FROM " + targetTable,
                "VALUES (1, 'Aaron', 17, 'Arches/Arches'), (2, 'Bill', 7, 'Buena'), (6, 'Carol', 9, 'Centreville'), (7, 'Dave', 22, 'Darbyshire/Darbyshire'), (8, 'Ed', 14, 'Etherville/Etherville'), (9, 'Fred', 30, 'Franklin')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeWithSimplifiedUnpredictablePredicates()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_predicates_target_" + randomNameSuffix();
        String sourceTable = "merge_predicates_source_" + randomNameSuffix();

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (id INT, customer VARCHAR, purchases INT, address VARCHAR)", targetTable)));

        assertUpdate(format("INSERT INTO %s (id, customer, purchases, address) VALUES (1, 'Dave', 11, 'Devon'), (2, 'Dave', 11, 'Darbyshire')", targetTable), 2);

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR)", sourceTable)));

        assertUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Darbyshire')", sourceTable), 1);

        assertUpdate(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        " ON t.customer = s.customer" +
                        "    WHEN MATCHED AND t.address <> 'Darbyshire' AND s.purchases * 2 > 20" +
                        "        THEN DELETE",
                1);

        assertQuery("SELECT * FROM " + targetTable, "VALUES (2, 'Dave', 11, 'Darbyshire')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeCasts()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_cast_target_" + randomNameSuffix();
        String sourceTable = "merge_cast_source_" + randomNameSuffix();

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (col1 INT, col2 DOUBLE, col3 INT, col4 BIGINT, col5 REAL, col6 DOUBLE)", targetTable)));

        assertUpdate(format("INSERT INTO %s VALUES (1, 2, 3, 4, 5, 6)", targetTable), 1);

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (col1 BIGINT, col2 REAL, col3 DOUBLE, col4 INT, col5 INT, col6 REAL)", sourceTable)));

        assertUpdate(format("INSERT INTO %s VALUES (2, 3, 4, 5, 6, 7)", sourceTable), 1);

        assertUpdate(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        "    ON (t.col1 + 1 = s.col1)" +
                        "    WHEN MATCHED THEN UPDATE SET col1 = s.col1, col2 = s.col2, col3 = s.col3, col4 = s.col4, col5 = s.col5, col6 = s.col6",
                1);

        assertQuery("SELECT * FROM " + targetTable, "VALUES (2, 3.0, 4, 5, 6.0, 7.0)");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeSubqueries()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE));

        String targetTable = "merge_nation_target_" + randomNameSuffix();
        String sourceTable = "merge_nation_source_" + randomNameSuffix();

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR)", targetTable)));

        assertUpdate(format("INSERT INTO %s (nation_name, region_name) VALUES ('FRANCE', 'EUROPE'), ('ALGERIA', 'AFRICA'), ('GERMANY', 'EUROPE')", targetTable), 3);

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR)", sourceTable)));

        assertUpdate(format("INSERT INTO %s VALUES ('ALGERIA', 'AFRICA'), ('FRANCE', 'EUROPE'), ('EGYPT', 'MIDDLE EAST'), ('RUSSIA', 'EUROPE')", sourceTable), 4);

        assertUpdate(
                format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        "    ON (t.nation_name = s.nation_name)" +
                        "    WHEN MATCHED AND t.nation_name > (SELECT name FROM tpch.tiny.region WHERE name = t.region_name AND name LIKE ('A%'))" +
                        "        THEN DELETE" +
                        "    WHEN NOT MATCHED AND s.region_name = 'EUROPE'" +
                        "        THEN INSERT VALUES(s.nation_name, (SELECT 'EUROPE'))",
                2);

        assertQuery("SELECT * FROM " + targetTable, "VALUES ('FRANCE', 'EUROPE'), ('GERMANY', 'EUROPE'), ('RUSSIA', 'EUROPE')");

        assertUpdate("DROP TABLE " + sourceTable);
        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeNonNullableColumns()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE) && hasBehavior(SUPPORTS_NOT_NULL_CONSTRAINT));

        String targetTable = "merge_non_nullable_target_" + randomNameSuffix();

        assertUpdate(createTableForWrites(format("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR NOT NULL)", targetTable)));

        assertUpdate(format("INSERT INTO %s (nation_name, region_name) VALUES ('FRANCE', 'EUROPE'), ('ALGERIA', 'AFRICA'), ('GERMANY', 'EUROPE')", targetTable), 3);

        // Show that updating using a null value fails
        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t\n", targetTable) +
                " USING (VALUES ('ALGERIA', 'AFRICA')) s(nation_name, region_name)\n" +
                " ON (t.nation_name = s.nation_name)\n" +
                " WHEN MATCHED THEN UPDATE SET region_name = NULL"))
                .hasMessage("NULL value not allowed for NOT NULL column: region_name");

        // Show that inserting using a null value fails
        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t\n", targetTable) +
                " USING (VALUES ('IMAGINARIA', 'AFRICA')) s(nation_name, region_name)\n" +
                " ON (t.nation_name = s.nation_name)\n" +
                " WHEN NOT MATCHED THEN INSERT (nation_name, region_name) VALUES ('IMAGINARIA', NULL)"))
                .hasMessage("NULL value not allowed for NOT NULL column: region_name");

        // Show that inserting using an implicit null value fails
        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t\n", targetTable) +
                " USING (VALUES ('IMAGINARIA', 'AFRICA')) s(nation_name, region_name)\n" +
                " ON (t.nation_name = s.nation_name)\n" +
                // The region_name is implicitly assigned null
                " WHEN NOT MATCHED THEN INSERT (nation_name) VALUES ('IMAGINARIA')"))
                .hasMessage("NULL value not allowed for NOT NULL column: region_name");

        // Show that if the updated value is provided by a function unpredicatably computing null,
        // the merge fails
        assertThatThrownBy(() -> computeActual(format("MERGE INTO %s t\n", targetTable) +
                " USING (VALUES ('ALGERIA', 'AFRICA')) s(nation_name, region_name)\n" +
                " ON (t.nation_name = s.nation_name)\n" +
                " WHEN MATCHED THEN UPDATE SET region_name = CAST(TRY(5/0) AS VARCHAR)"))
                .hasMessage("NULL value not allowed for NOT NULL column: region_name");

        assertUpdate("DROP TABLE " + targetTable);
    }

    @Test
    public void testMergeAllColumnsReversed()
    {
        skipTestUnless(hasBehavior(SUPPORTS_MERGE) && hasBehavior(SUPPORTS_NOT_NULL_CONSTRAINT));

        String targetTable = "merge_update_columns_reversed_" + randomNameSuffix();
        assertUpdate(createTableForWrites("CREATE TABLE " + targetTable + " (a, b, c) AS VALUES (1, 2, 3)"), 1);
        assertUpdate(
                """
                MERGE INTO %s t USING (VALUES(1)) AS s(a) ON (t.a = s.a)
                    WHEN MATCHED THEN UPDATE
                        SET c = 100, b = 42, a = 0
                """.formatted(targetTable),
                1);
        assertQuery("SELECT * FROM " + targetTable, "VALUES (0, 42, 100)");

        assertUpdate("DROP TABLE " + targetTable);
    }

    private void verifyUnsupportedTypeException(Throwable exception, String trinoTypeName)
    {
        String typeNameBase = trinoTypeName.replaceFirst("\\(.*", "");
        String expectedMessagePart = format("(%1$s.*not (yet )?supported)|((?i)unsupported.*%1$s)|((?i)not supported.*%1$s)", Pattern.quote(typeNameBase));
        assertThat(exception)
                .hasMessageFindingMatch(expectedMessagePart)
                .satisfies(e -> assertThat(getTrinoExceptionCause(e)).hasMessageFindingMatch(expectedMessagePart));
    }

    @Test
    public void testMaterializedViewColumnName()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW));

        for (String columnName : testColumnNameDataProvider()) {
            testMaterializedViewColumnName(columnName, requiresDelimiting(columnName));
        }
    }

    private void testMaterializedViewColumnName(String columnName, boolean delimited)
    {
        String nameInSql = columnName;
        if (delimited) {
            nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
        }
        String viewName = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "_") + "_" + randomNameSuffix();

        try {
            assertUpdate("CREATE MATERIALIZED VIEW " + viewName + " AS SELECT 'sample value' key, 'abc' " + nameInSql);
        }
        catch (RuntimeException e) {
            if (isColumnNameRejected(e, columnName, delimited)) {
                // It is OK if give column name is not allowed and is clearly rejected by the connector.
                return;
            }
            throw e;
        }

        assertUpdate("REFRESH MATERIALIZED VIEW " + viewName, 1);
        assertQuery("SELECT * FROM " + viewName, "VALUES ('sample value', 'abc')");

        assertUpdate("DROP MATERIALIZED VIEW " + viewName);
    }

    @Test
    public void testCreateFunction()
    {
        if (!hasBehavior(SUPPORTS_CREATE_FUNCTION)) {
            String catalog = getQueryRunner().getDefaultSession().getCatalog().orElseThrow();
            String schema = getQueryRunner().getDefaultSession().getSchema().orElseThrow();
            assertQueryFails(
                    "CREATE FUNCTION " + catalog + "." + schema + ".test_create_function" + randomNameSuffix() + "(x integer) RETURNS bigint COMMENT 't42' RETURN x * 42",
                    "This connector does not support creating functions");
            return;
        }

        String name = "test_" + randomNameSuffix();
        String name2 = "test_" + randomNameSuffix();
        String name3 = "test_" + randomNameSuffix();

        assertQueryFails("SHOW CREATE FUNCTION " + name, "line 1:1: Function not found");

        assertUpdate("CREATE FUNCTION " + name + "(x integer) RETURNS bigint COMMENT 't42' RETURN x * 42");

        assertQuery("SELECT " + name + "(99)", "SELECT 4158");
        assertQueryFails("SELECT " + name + "(2.9)", ".*Unexpected parameters.*");

        assertUpdate("CREATE FUNCTION " + name + "(x double) RETURNS double COMMENT 't88' RETURN x * 8.8");

        assertThat(query("SHOW FUNCTIONS"))
                .result()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row(name, "bigint", "integer", "scalar", true, "t42")
                        .row(name, "double", "double", "scalar", true, "t88")
                        .build());

        String integerFunctionSql =
                """
                CREATE FUNCTION %s(x integer)
                RETURNS bigint
                COMMENT 't42'
                RETURN (x * 42)
                """.strip().formatted(name);
        String doubleFunctionSql =
                """
                CREATE FUNCTION %s(x double)
                RETURNS double
                COMMENT 't88'
                RETURN (x * DECIMAL '8.8')
                """.strip().formatted(name);
        assertThat(computeActual("SHOW CREATE FUNCTION " + name).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(integerFunctionSql, doubleFunctionSql);

        assertQuery("SELECT " + name + "(99)", "SELECT 4158");
        assertQuery("SELECT " + name + "(2.9)", "SELECT 25.52");

        assertQueryFails("CREATE FUNCTION " + name + "(x int) RETURNS bigint RETURN x", "line 1:1: Function already exists");

        assertQuery("SELECT " + name + "(99)", "SELECT 4158");
        assertQuery("SELECT " + name + "(2.9)", "SELECT 25.52");

        assertUpdate("CREATE OR REPLACE FUNCTION " + name + "(x bigint) RETURNS bigint RETURN x * 23");
        assertUpdate("CREATE FUNCTION " + name2 + "(s varchar) RETURNS varchar RETURN 'Hello ' || s");

        assertThat(query("SHOW FUNCTIONS"))
                .result()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row(name, "bigint", "integer", "scalar", true, "t42")
                        .row(name, "bigint", "bigint", "scalar", true, "")
                        .row(name, "double", "double", "scalar", true, "t88")
                        .row(name2, "varchar", "varchar", "scalar", true, "")
                        .build());

        String bigintFunctionSql =
                """
                CREATE FUNCTION %s(x bigint)
                RETURNS bigint
                RETURN (x * 23)
                """.strip().formatted(name);
        assertThat(computeActual("SHOW CREATE FUNCTION " + name).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder(integerFunctionSql, doubleFunctionSql, bigintFunctionSql);

        assertQuery("SELECT " + name + "(99)", "SELECT 4158");
        assertQuery("SELECT " + name + "(cast(99 as bigint))", "SELECT 2277");
        assertQuery("SELECT " + name + "(2.9)", "SELECT 25.52");
        assertQuery("SELECT " + name2 + "('world')", "SELECT 'Hello world'");

        assertQuery("SELECT sum(" + name + "(orderkey)) FROM orders", "SELECT sum(orderkey * 23) FROM orders");

        assertUpdate("CREATE FUNCTION " + name3 + "() RETURNS double NOT DETERMINISTIC RETURN random()");

        assertThat(query("SHOW FUNCTIONS"))
                .result()
                .skippingTypesCheck()
                .containsAll(resultBuilder(getSession())
                        .row(name3, "double", "", "scalar", false, "")
                        .build());

        assertThat(query("SHOW FUNCTIONS FROM " + computeScalar("SELECT current_path")))
                .result()
                .skippingTypesCheck()
                .matches(resultBuilder(getSession())
                        .row(name, "bigint", "integer", "scalar", true, "t42")
                        .row(name, "bigint", "bigint", "scalar", true, "")
                        .row(name, "double", "double", "scalar", true, "t88")
                        .row(name2, "varchar", "varchar", "scalar", true, "")
                        .row(name3, "double", "", "scalar", false, "")
                        .build());

        assertThat(computeActual("SHOW CREATE FUNCTION " + name3).getOnlyValue())
                .isEqualTo(
                        """
                        CREATE FUNCTION %s()
                        RETURNS double
                        NOT DETERMINISTIC
                        RETURN random()
                        """.strip().formatted(name3));

        assertQueryFails("DROP FUNCTION " + name + "(varchar)", "line 1:1: Function not found");
        assertUpdate("DROP FUNCTION " + name + "(z bigint)");
        assertUpdate("DROP FUNCTION " + name + "(double)");
        assertQueryFails("DROP FUNCTION " + name + "(bigint)", "line 1:1: Function not found");
        assertUpdate("DROP FUNCTION IF EXISTS " + name + "(bigint)");
        assertUpdate("DROP FUNCTION " + name + "(int)");
        assertUpdate("DROP FUNCTION " + name2 + "(varchar)");
        assertQueryFails("DROP FUNCTION " + name2 + "(varchar)", "line 1:1: Function not found");
        assertUpdate("DROP FUNCTION " + name3 + "()");
        assertQueryFails("DROP FUNCTION " + name3 + "()", "line 1:1: Function not found");

        assertThat(query("SHOW FUNCTIONS FROM " + computeScalar("SELECT current_path")))
                .returnsEmptyResult();

        // verify stored functions cannot see inline functions
        String myAbs = "my_abs_" + randomNameSuffix();
        assertUpdate("CREATE FUNCTION " + myAbs + "(x integer) RETURNS integer RETURN abs(x)");
        // test with inline function first as FunctionManager caches compiled implementations
        assertQuery("WITH FUNCTION abs(x integer) RETURNS integer RETURN x * 2 SELECT " + myAbs + "(-33)", "SELECT 33");
        assertQuery("SELECT " + myAbs + "(-33)", "SELECT 33");

        String wrapMyAbs = "wrap_my_abs_" + randomNameSuffix();
        assertUpdate("CREATE FUNCTION " + wrapMyAbs + "(x integer) RETURNS integer RETURN " + myAbs + "(x)");
        // test with inline function first as FunctionManager caches compiled implementations
        assertQuery("WITH FUNCTION " + myAbs + "(x integer) RETURNS integer RETURN x * 2 SELECT " + wrapMyAbs + "(-33)", "SELECT 33");
        assertQuery("SELECT " + wrapMyAbs + "(-33)", "SELECT 33");
        assertUpdate("DROP FUNCTION " + myAbs + "(integer)");
        assertUpdate("DROP FUNCTION " + wrapMyAbs + "(integer)");

        // verify mutually recursive functions are not allowed
        String recursive1 = "recursive1_" + randomNameSuffix();
        String recursive2 = "recursive2_" + randomNameSuffix();
        assertUpdate("CREATE FUNCTION " + recursive1 + "(x integer) RETURNS integer RETURN x");
        assertUpdate("CREATE FUNCTION " + recursive2 + "(x integer) RETURNS integer RETURN " + recursive1 + "(x)");
        assertUpdate("CREATE OR REPLACE FUNCTION " + recursive1 + "(x integer) RETURNS integer RETURN " + recursive2 + "(x)");
        assertQueryFails("SELECT " + recursive1 + "(42)", "Recursive language functions are not supported: " + recursive1 + "\\(integer\\):integer");
        assertQueryFails("SELECT " + recursive2 + "(42)", "Recursive language functions are not supported: " + recursive2 + "\\(integer\\):integer");
        assertUpdate("DROP FUNCTION " + recursive1 + "(integer)");
        assertUpdate("DROP FUNCTION " + recursive2 + "(integer)");

        // verify exception code when function references another, not existing function
        String wrappingFunction = "wrapping_" + randomNameSuffix();
        String wrappedFunction = "wrapped_" + randomNameSuffix();

        // wrapped_() not yet registered
        assertThat(query("CREATE FUNCTION " + wrappingFunction + "() RETURNS integer RETURN " + wrappedFunction + "()")).failure()
                .hasMessage("line 1:62: Function '" + wrappedFunction + "' not registered")
                .hasErrorCode(FUNCTION_NOT_FOUND);

        assertUpdate("CREATE FUNCTION " + wrappedFunction + "() RETURNS integer RETURN 42");
        assertUpdate("CREATE FUNCTION " + wrappingFunction + "() RETURNS integer RETURN " + wrappedFunction + "()");
        assertQuery("SELECT " + wrappingFunction + "()", "SELECT 42");
        assertUpdate("DROP FUNCTION " + wrappedFunction + "()");

        // wrapped_() dropped
        assertThat(query("SELECT " + wrappingFunction + "()")).failure()
                .hasMessage("line 1:8: Function '" + wrappedFunction + "' not registered")
                .hasErrorCode(FUNCTION_NOT_FOUND);
    }

    @Test
    public void testProjectionPushdown()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_ROW_TYPE));

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_projection_pushdown_",
                "(id BIGINT, root ROW(f1 BIGINT, f2 BIGINT))",
                ImmutableList.of("(1, ROW(1, 2))", "(2, NULl)", "(3, ROW(NULL, 4))"))) {
            String selectQuery = "SELECT id, root.f1 FROM " + testTable.getName();
            String expectedResult = "VALUES (BIGINT '1', BIGINT '1'), (BIGINT '2', NULL), (BIGINT '3', NULL)";

            if (!hasBehavior(SUPPORTS_DEREFERENCE_PUSHDOWN)) {
                assertThat(query(selectQuery))
                        .matches(expectedResult)
                        .isNotFullyPushedDown(ProjectNode.class);
            }
            else {
                // With Projection Pushdown enabled
                assertThat(query(selectQuery))
                        .matches(expectedResult)
                        .isFullyPushedDown();

                // With Projection Pushdown disabled
                Session sessionWithoutPushdown = sessionWithProjectionPushdownDisabled(getSession());
                assertThat(query(sessionWithoutPushdown, selectQuery))
                        .matches(expectedResult)
                        .isNotFullyPushedDown(ProjectNode.class);
            }
        }
    }

    @Test
    public void testProjectionWithCaseSensitiveField()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_DEREFERENCE_PUSHDOWN));

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_projection_with_case_sensitive_field_",
                "(id BIGINT, a ROW(\"UPPER_CASE\" BIGINT, \"lower_case\" BIGINT, \"MiXeD_cAsE\" BIGINT))",
                ImmutableList.of("(1, ROW(2, 3, 4))", "(5, ROW(6, 7, 8))"))) {
            String expected = "VALUES (BIGINT '2', BIGINT '3', BIGINT '4'), (BIGINT '6', BIGINT '7', BIGINT '8')";
            assertThat(query("SELECT a.UPPER_CASE, a.lower_case, a.MiXeD_cAsE FROM " + testTable.getName()))
                    .matches(expected)
                    .isFullyPushedDown();
            assertThat(query("SELECT a.upper_case, a.lower_case, a.mixed_case FROM " + testTable.getName()))
                    .matches(expected)
                    .isFullyPushedDown();
            assertThat(query("SELECT a.UPPER_CASE, a.LOWER_CASE, a.MIXED_CASE FROM " + testTable.getName()))
                    .matches(expected)
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testProjectionPushdownMultipleRows()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_DEREFERENCE_PUSHDOWN));

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_projection_pushdown_multiple_rows_",
                "(id BIGINT, nested1 ROW(child1 BIGINT, child2 VARCHAR, child3 BIGINT), nested2 ROW(child1 DOUBLE, child2 BOOLEAN, child3 DATE))",
                ImmutableList.of(
                        "(1, ROW(10, 'a', 100), ROW(10.10, true, DATE '2023-04-19'))",
                        "(2, ROW(20, 'b', 200), ROW(20.20, false, DATE '1990-04-20'))",
                        "(4, ROW(40, NULL, 400), NULL)",
                        "(5, NULL, ROW(NULL, true, NULL))"))) {
            // Select one field from one row field
            assertThat(query("SELECT id, nested1.child1 FROM " + testTable.getName()))
                    .matches("VALUES (BIGINT '1', BIGINT '10'), (BIGINT '2', BIGINT '20'), (BIGINT '4', BIGINT '40'), (BIGINT '5', NULL)")
                    .isFullyPushedDown();
            assertThat(query("SELECT nested2.child3, id FROM " + testTable.getName()))
                    .matches("VALUES (DATE '2023-04-19', BIGINT '1'), (DATE '1990-04-20', BIGINT '2'), (NULL, BIGINT '4'), (NULL, BIGINT '5')")
                    .isFullyPushedDown();

            // Select one field each from multiple row fields
            assertThat(query("SELECT nested2.child1, id, nested1.child2 FROM " + testTable.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES (DOUBLE '10.10', BIGINT '1', 'a'), (DOUBLE '20.20', BIGINT '2', 'b'), (NULL, BIGINT '4', NULL), (NULL, BIGINT '5', NULL)")
                    .isFullyPushedDown();

            // Select multiple fields from one row field
            assertThat(query("SELECT nested1.child3, id, nested1.child2 FROM " + testTable.getName()))
                    .skippingTypesCheck()
                    .matches("VALUES (BIGINT '100', BIGINT '1', 'a'), (BIGINT '200', BIGINT '2', 'b'), (BIGINT '400', BIGINT '4', NULL), (NULL, BIGINT '5', NULL)")
                    .isFullyPushedDown();
            assertThat(query("SELECT nested2.child2, nested2.child3, id FROM " + testTable.getName()))
                    .matches("VALUES (true, DATE '2023-04-19' , BIGINT '1'), (false, DATE '1990-04-20', BIGINT '2'), (NULL, NULL, BIGINT '4'), (true, NULL, BIGINT '5')")
                    .isFullyPushedDown();

            // Select multiple fields from multiple row fields
            assertThat(query("SELECT id, nested2.child1, nested1.child3, nested2.child2, nested1.child1 FROM " + testTable.getName()))
                    .matches("VALUES (BIGINT '1', DOUBLE '10.10', BIGINT '100', true, BIGINT '10'), (BIGINT '2', DOUBLE '20.20', BIGINT '200', false, BIGINT '20'), (BIGINT '4', NULL, BIGINT '400', NULL, BIGINT '40'), (BIGINT '5', NULL, NULL, true, NULL)")
                    .isFullyPushedDown();

            // Select only nested fields
            assertThat(query("SELECT nested2.child2, nested1.child3 FROM " + testTable.getName()))
                    .matches("VALUES (true, BIGINT '100'), (false, BIGINT '200'), (NULL, BIGINT '400'), (true, NULL)")
                    .isFullyPushedDown();
        }
    }

    @Test
    public void testProjectionPushdownWithHighlyNestedData()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_DEREFERENCE_PUSHDOWN));

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_highly_nested_data_",
                "(id INT, row1_t ROW(f1 INT, f2 INT, row2_t ROW (f1 INT, f2 INT, row3_t ROW(f1 INT, f2 INT))))",
                ImmutableList.of("(1, ROW(2, 3, ROW(4, 5, ROW(6, 7))))",
                        "(11, ROW(12, 13, ROW(14, 15, ROW(16, 17))))",
                        "(21, ROW(22, 23, ROW(24, 25, ROW(26, 27))))"))) {
            // Test select projected columns, with and without their parent column
            assertQuery("SELECT id, row1_t.row2_t.row3_t.f2 FROM " + testTable.getName(), "VALUES (1, 7), (11, 17), (21, 27)");
            assertQuery("SELECT id, row1_t.row2_t.row3_t.f2, CAST(row1_t AS JSON) FROM " + testTable.getName(),
                    "VALUES (1, 7, '{\"f1\":2,\"f2\":3,\"row2_t\":{\"f1\":4,\"f2\":5,\"row3_t\":{\"f1\":6,\"f2\":7}}}'), " +
                            "(11, 17, '{\"f1\":12,\"f2\":13,\"row2_t\":{\"f1\":14,\"f2\":15,\"row3_t\":{\"f1\":16,\"f2\":17}}}'), " +
                            "(21, 27, '{\"f1\":22,\"f2\":23,\"row2_t\":{\"f1\":24,\"f2\":25,\"row3_t\":{\"f1\":26,\"f2\":27}}}')");

            // Test predicates on immediate child column and deeper nested column
            assertQuery("SELECT id, CAST(row1_t.row2_t.row3_t AS JSON) FROM " + testTable.getName() + " WHERE row1_t.row2_t.row3_t.f2 = 27", "VALUES (21, '{\"f1\":26,\"f2\":27}')");
            assertQuery("SELECT id, CAST(row1_t.row2_t.row3_t AS JSON) FROM " + testTable.getName() + " WHERE row1_t.row2_t.row3_t.f2 > 20", "VALUES (21, '{\"f1\":26,\"f2\":27}')");
            assertQuery("SELECT id, CAST(row1_t AS JSON) FROM " + testTable.getName() + " WHERE row1_t.row2_t.row3_t.f2 = 27",
                    "VALUES (21, '{\"f1\":22,\"f2\":23,\"row2_t\":{\"f1\":24,\"f2\":25,\"row3_t\":{\"f1\":26,\"f2\":27}}}')");
            assertQuery("SELECT id, CAST(row1_t AS JSON) FROM " + testTable.getName() + " WHERE row1_t.row2_t.row3_t.f2 > 20",
                    "VALUES (21, '{\"f1\":22,\"f2\":23,\"row2_t\":{\"f1\":24,\"f2\":25,\"row3_t\":{\"f1\":26,\"f2\":27}}}')");

            // Test predicates on parent columns
            assertQuery("SELECT id, row1_t.row2_t.row3_t.f1 FROM " + testTable.getName() + " WHERE row1_t.row2_t.row3_t = ROW(16, 17)", "VALUES (11, 16)");
            assertQuery("SELECT id, row1_t.row2_t.row3_t.f1 FROM " + testTable.getName() + " WHERE row1_t = ROW(22, 23, ROW(24, 25, ROW(26, 27)))", "VALUES (21, 26)");
        }
    }

    @Test
    public void testProjectionPushdownReadsLessData()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_DEREFERENCE_PUSHDOWN));

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_projection_pushdown_reads_less_data_",
                "AS SELECT val AS id, CAST(ROW(val + 1, val + 2) AS ROW(leaf1 BIGINT, leaf2 BIGINT)) AS root FROM UNNEST(SEQUENCE(1, 10)) AS t(val)")) {
            MaterializedResult expectedResult = computeActual("SELECT val + 2 FROM UNNEST(SEQUENCE(1, 10)) AS t(val)");
            String selectQuery = "SELECT root.leaf2 FROM " + testTable.getName();
            Session sessionWithoutSmallFileThreshold = withoutSmallFileThreshold(getSession());
            Session sessionWithoutPushdown = sessionWithProjectionPushdownDisabled(sessionWithoutSmallFileThreshold);

            assertQueryStats(
                    sessionWithoutSmallFileThreshold,
                    selectQuery,
                    statsWithPushdown -> {
                        DataSize physicalInputDataSizeWithPushdown = statsWithPushdown.getPhysicalInputDataSize();
                        DataSize processedDataSizeWithPushdown = statsWithPushdown.getProcessedInputDataSize();
                        assertQueryStats(
                                sessionWithoutPushdown,
                                selectQuery,
                                statsWithoutPushdown -> {
                                    if (supportsPhysicalPushdown()) {
                                        assertThat(statsWithoutPushdown.getPhysicalInputDataSize()).isGreaterThan(physicalInputDataSizeWithPushdown);
                                    }
                                    else {
                                        // TODO https://github.com/trinodb/trino/issues/17201
                                        assertThat(statsWithoutPushdown.getPhysicalInputDataSize()).isEqualTo(physicalInputDataSizeWithPushdown);
                                    }
                                    assertThat(statsWithoutPushdown.getProcessedInputDataSize()).isGreaterThan(processedDataSizeWithPushdown);
                                },
                                results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(expectedResult.getOnlyColumnAsSet()));
                    },
                    results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(expectedResult.getOnlyColumnAsSet()));
        }
    }

    @Test
    public void testProjectionPushdownPhysicalInputSize()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA) && hasBehavior(SUPPORTS_DEREFERENCE_PUSHDOWN));

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_projection_pushdown_physical_input_size_",
                "AS SELECT val AS id, CAST(ROW(val + 1, val + 2) AS ROW(leaf1 BIGINT, leaf2 BIGINT)) AS root FROM UNNEST(SEQUENCE(1, 10)) AS t(val)")) {
            // Verify that the physical input size is smaller when reading the root.leaf1 field compared to reading the root field
            Session sessionWithoutSmallFileThreshold = withoutSmallFileThreshold(getSession());
            assertQueryStats(
                    sessionWithoutSmallFileThreshold,
                    "SELECT root FROM " + testTable.getName(),
                    statsWithSelectRootField -> {
                        assertQueryStats(
                                sessionWithoutSmallFileThreshold,
                                "SELECT root.leaf1 FROM " + testTable.getName(),
                                statsWithSelectLeafField -> {
                                    if (supportsPhysicalPushdown()) {
                                        assertThat(statsWithSelectLeafField.getPhysicalInputDataSize()).isLessThan(statsWithSelectRootField.getPhysicalInputDataSize());
                                    }
                                    else {
                                        // TODO https://github.com/trinodb/trino/issues/17201
                                        assertThat(statsWithSelectLeafField.getPhysicalInputDataSize()).isEqualTo(statsWithSelectRootField.getPhysicalInputDataSize());
                                    }
                                },
                                results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(computeActual("SELECT val + 1 FROM UNNEST(SEQUENCE(1, 10)) AS t(val)").getOnlyColumnAsSet()));
                    },
                    results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(computeActual("SELECT ROW(val + 1, val + 2) FROM UNNEST(SEQUENCE(1, 10)) AS t(val)").getOnlyColumnAsSet()));

            // Verify that the physical input size is the same when reading the root field compared to reading both the root and root.leaf1 fields
            assertQueryStats(
                    sessionWithoutSmallFileThreshold,
                    "SELECT root FROM " + testTable.getName(),
                    statsWithSelectRootField -> {
                        assertQueryStats(
                                sessionWithoutSmallFileThreshold,
                                "SELECT root, root.leaf1 FROM " + testTable.getName(),
                                statsWithSelectRootAndLeafField -> {
                                    assertThat(statsWithSelectRootAndLeafField.getPhysicalInputDataSize()).isEqualTo(statsWithSelectRootField.getPhysicalInputDataSize());
                                },
                                results -> assertEqualsIgnoreOrder(results.getMaterializedRows(), computeActual("SELECT ROW(val + 1, val + 2), val + 1 FROM UNNEST(SEQUENCE(1, 10)) AS t(val)").getMaterializedRows()));
                    },
                    results -> assertThat(results.getOnlyColumnAsSet()).isEqualTo(computeActual("SELECT ROW(val + 1, val + 2) FROM UNNEST(SEQUENCE(1, 10)) AS t(val)").getOnlyColumnAsSet()));
        }
    }

    protected static void skipTestUnless(boolean requirement)
    {
        if (!requirement) {
            abort("requirement not met");
        }
    }

    protected Consumer<Plan> assertPartialLimitWithPreSortedInputsCount(Session session, int expectedCount)
    {
        return plan -> {
            int actualCount = searchFrom(plan.getRoot())
                    .where(node -> node instanceof LimitNode && ((LimitNode) node).isPartial() && ((LimitNode) node).requiresPreSortedInputs())
                    .findAll()
                    .size();
            if (actualCount != expectedCount) {
                Metadata metadata = getDistributedQueryRunner().getPlannerContext().getMetadata();
                FunctionManager functionManager = getDistributedQueryRunner().getPlannerContext().getFunctionManager();
                String formattedPlan = textLogicalPlan(plan.getRoot(), metadata, functionManager, StatsAndCosts.empty(), session, 0, false);
                throw new AssertionError(format(
                        "Expected [\n%s\n] partial limit but found [\n%s\n] partial limit. Actual plan is [\n\n%s\n]",
                        expectedCount,
                        actualCount,
                        formattedPlan));
            }
        };
    }

    protected void withMockTableListing(String forSchema, Function<ConnectorSession, List<String>> listing, Runnable closure)
    {
        requireNonNull(forSchema, "forSchema is null");
        requireNonNull(listing, "listing is null");

        checkState(mockTableListings.putIfAbsent(forSchema, listing) == null, "Listing function already registered for [%s]", forSchema);
        try {
            closure.run();
        }
        finally {
            mockTableListings.remove(forSchema, listing);
        }
    }

    protected String createSchemaSql(String schemaName)
    {
        return "CREATE SCHEMA " + schemaName;
    }

    protected boolean supportsPhysicalPushdown()
    {
        return true;
    }

    protected Session sessionWithProjectionPushdownDisabled(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "projection_pushdown_enabled", "false")
                .build();
    }

    protected Session withoutSmallFileThreshold(Session session)
    {
        throw new UnsupportedOperationException();
    }

    protected static final class DataMappingTestSetup
    {
        private final String trinoTypeName;
        private final String sampleValueLiteral;
        private final String highValueLiteral;

        private final boolean unsupportedType;

        public DataMappingTestSetup(String trinoTypeName, String sampleValueLiteral, String highValueLiteral)
        {
            this(trinoTypeName, sampleValueLiteral, highValueLiteral, false);
        }

        private DataMappingTestSetup(String trinoTypeName, String sampleValueLiteral, String highValueLiteral, boolean unsupportedType)
        {
            this.trinoTypeName = requireNonNull(trinoTypeName, "trinoTypeName is null");
            this.sampleValueLiteral = requireNonNull(sampleValueLiteral, "sampleValueLiteral is null");
            this.highValueLiteral = requireNonNull(highValueLiteral, "highValueLiteral is null");
            this.unsupportedType = unsupportedType;
        }

        public String getTrinoTypeName()
        {
            return trinoTypeName;
        }

        public String getSampleValueLiteral()
        {
            return sampleValueLiteral;
        }

        public String getHighValueLiteral()
        {
            return highValueLiteral;
        }

        public boolean isUnsupportedType()
        {
            return unsupportedType;
        }

        public DataMappingTestSetup asUnsupported()
        {
            return new DataMappingTestSetup(
                    trinoTypeName,
                    sampleValueLiteral,
                    highValueLiteral,
                    true);
        }

        @Override
        public String toString()
        {
            // Used for test case labels in IDE
            return trinoTypeName + (unsupportedType ? "!" : "") +
                    ":" + sampleValueLiteral.replaceAll("[^a-zA-Z0-9_-]", "");
        }
    }
}
