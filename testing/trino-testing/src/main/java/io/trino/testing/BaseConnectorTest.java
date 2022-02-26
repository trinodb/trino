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
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.units.Duration;
import io.trino.FeaturesConfig.JoinDistributionType;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.server.BasicQueryInfo;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.SystemSessionProperties.IGNORE_STATS_CALCULATOR_FAILURES;
import static io.trino.connector.informationschema.InformationSchemaTable.INFORMATION_SCHEMA;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.planprinter.PlanPrinter.textLogicalPlan;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.QueryAssertions.getTrinoExceptionCause;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ADD_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ARRAY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_MATERIALIZED_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DROP_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_MULTI_STATEMENT_WRITES;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NEGATIVE_DATE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_NOT_NULL_CONSTRAINT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_MATERIALIZED_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ROW_LEVEL_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TRUNCATE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_UPDATE;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Generic test for connectors.
 */
public abstract class BaseConnectorTest
        extends AbstractTestQueries
{
    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    protected final boolean supportsCreateSchema()
    {
        return hasBehavior(SUPPORTS_CREATE_SCHEMA);
    }

    private boolean supportsDropSchema()
    {
        // A connector either supports CREATE SCHEMA and DROP SCHEMA or none of them.
        return supportsCreateSchema();
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    protected final boolean supportsCreateTable()
    {
        return hasBehavior(SUPPORTS_CREATE_TABLE);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    protected final boolean supportsInsert()
    {
        return hasBehavior(SUPPORTS_INSERT);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    protected final boolean supportsDelete()
    {
        return hasBehavior(SUPPORTS_DELETE);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    protected final boolean supportsViews()
    {
        return hasBehavior(SUPPORTS_CREATE_VIEW);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    protected final boolean supportsArrays()
    {
        return hasBehavior(SUPPORTS_ARRAY);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    protected final boolean supportsCommentOnTable()
    {
        return hasBehavior(SUPPORTS_COMMENT_ON_TABLE);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    protected final boolean supportsCommentOnColumn()
    {
        return hasBehavior(SUPPORTS_COMMENT_ON_COLUMN);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    protected boolean supportsRenameTable()
    {
        return hasBehavior(SUPPORTS_RENAME_TABLE);
    }

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
     * Ensure the tests are run with {@link DistributedQueryRunner}. E.g. {@link LocalQueryRunner} takes some
     * shortcuts, not exercising certain aspects.
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
        String schemaName = "test_schema_create_" + randomTableSuffix();
        if (!supportsCreateSchema()) {
            assertQueryFails("CREATE SCHEMA " + schemaName, "This connector does not support creating schemas");
            return;
        }
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schemaName);
        assertUpdate("CREATE SCHEMA " + schemaName);

        // verify listing of new schema
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);

        // verify SHOW CREATE SCHEMA works
        assertThat((String) computeScalar("SHOW CREATE SCHEMA " + schemaName))
                .startsWith(format("CREATE SCHEMA %s.%s", getSession().getCatalog().orElseThrow(), schemaName));

        // try to create duplicate schema
        assertQueryFails("CREATE SCHEMA " + schemaName, format("line 1:1: Schema '.*\\.%s' already exists", schemaName));

        // cleanup
        assertUpdate("DROP SCHEMA " + schemaName);

        // verify DROP SCHEMA for non-existing schema
        assertQueryFails("DROP SCHEMA " + schemaName, format("line 1:1: Schema '.*\\.%s' does not exist", schemaName));
    }

    @Test
    public void testDropNonEmptySchema()
    {
        String schemaName = "test_drop_non_empty_schema_" + randomTableSuffix();
        if (!supportsDropSchema()) {
            return;
        }

        try {
            assertUpdate("CREATE SCHEMA " + schemaName);
            assertUpdate("CREATE TABLE " + schemaName + ".t(x int)");
            assertQueryFails("DROP SCHEMA " + schemaName, ".*Cannot drop non-empty schema '\\Q" + schemaName + "\\E'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + ".t");
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
                "WHERE orderkey BETWEEN 10 AND 50");
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
                : "\\[5 by \\((?i:nationkey) DESC NULLS LAST\\)]";

        assertExplain(
                "EXPLAIN SELECT name FROM nation ORDER BY nationkey DESC NULLS LAST LIMIT 5",
                expectedPattern);
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

    /**
     * Test interactions between optimizer (including CBO), scheduling and connector metadata APIs.
     */
    @Test(timeOut = 300_000, dataProvider = "joinDistributionTypes")
    public void testJoinWithEmptySides(JoinDistributionType joinDistributionType)
    {
        Session session = noJoinReordering(joinDistributionType);
        // empty build side
        assertQuery(session, "SELECT count(*) FROM nation JOIN region ON nation.regionkey = region.regionkey AND region.name = ''", "VALUES 0");
        assertQuery(session, "SELECT count(*) FROM nation JOIN region ON nation.regionkey = region.regionkey AND region.regionkey < 0", "VALUES 0");
        // empty probe side
        assertQuery(session, "SELECT count(*) FROM region JOIN nation ON nation.regionkey = region.regionkey AND region.name = ''", "VALUES 0");
        assertQuery(session, "SELECT count(*) FROM nation JOIN region ON nation.regionkey = region.regionkey AND region.regionkey < 0", "VALUES 0");
    }

    @DataProvider
    public Object[][] joinDistributionTypes()
    {
        return Stream.of(JoinDistributionType.values())
                .collect(toDataProvider());
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
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
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
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    public void testView()
    {
        if (!supportsViews()) {
            assertQueryFails("CREATE VIEW nation_v AS SELECT * FROM nation", "This connector does not support creating views");
            return;
        }

        @Language("SQL") String query = "SELECT orderkey, orderstatus, (totalprice / 2) half FROM orders";

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        String testView = "test_view_" + randomTableSuffix();
        String testViewWithComment = "test_view_with_comment_" + randomTableSuffix();
        assertUpdate("CREATE VIEW " + testView + " AS SELECT 123 x");
        assertUpdate("CREATE OR REPLACE VIEW " + testView + " AS " + query);

        assertUpdate("CREATE VIEW " + testViewWithComment + " COMMENT 'orders' AS SELECT 123 x");
        assertUpdate("CREATE OR REPLACE VIEW " + testViewWithComment + " COMMENT 'orders' AS " + query);

        // verify comment
        MaterializedResult materializedRows = computeActual("SHOW CREATE VIEW " + testViewWithComment);
        assertThat((String) materializedRows.getOnlyValue()).contains("COMMENT 'orders'");
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
                .projected(0) // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'orderkey', 'orderstatus', 'half'");

        assertThat(query("DESCRIBE " + testView))
                .projected(0) // column types can very between connectors
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
    }

    @Test
    public void testViewCaseSensitivity()
    {
        skipTestUnless(supportsViews());

        String upperCaseView = "test_view_uppercase_" + randomTableSuffix();
        String mixedCaseView = "test_view_mixedcase_" + randomTableSuffix();

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

        QualifiedObjectName view = new QualifiedObjectName(
                getSession().getCatalog().orElseThrow(),
                getSession().getSchema().orElseThrow(),
                "test_materialized_view_" + randomTableSuffix());
        QualifiedObjectName otherView = new QualifiedObjectName(
                getSession().getCatalog().orElseThrow(),
                "other_schema",
                "test_materialized_view_" + randomTableSuffix());
        QualifiedObjectName viewWithComment = new QualifiedObjectName(
                getSession().getCatalog().orElseThrow(),
                getSession().getSchema().orElseThrow(),
                "test_materialized_view_with_comment_" + randomTableSuffix());

        createTestingMaterializedView(view, Optional.empty());
        createTestingMaterializedView(otherView, Optional.of("sarcastic comment"));
        createTestingMaterializedView(viewWithComment, Optional.of("mv_comment"));

        // verify comment
        MaterializedResult materializedRows = computeActual("SHOW CREATE MATERIALIZED VIEW " + viewWithComment);
        assertThat((String) materializedRows.getOnlyValue()).contains("COMMENT 'mv_comment'");
        assertThat(query(
                "SELECT table_name, comment FROM system.metadata.table_comments " +
                        "WHERE catalog_name = '" + view.getCatalogName() + "' AND " +
                        "schema_name = '" + view.getSchemaName() + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + view.getObjectName() + "', null), ('" + viewWithComment.getObjectName() + "', 'mv_comment')");

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
                .containsAll("VALUES '" + view.getObjectName() + "'");
        // information_schema.tables without table_name filter
        assertThat(query(
                "SELECT table_name, table_type FROM information_schema.tables " +
                        "WHERE table_schema = '" + view.getSchemaName() + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + view.getObjectName() + "', 'BASE TABLE')"); // TODO table_type should probably be "* VIEW"
        // information_schema.tables with table_name filter
        assertQuery(
                "SELECT table_name, table_type FROM information_schema.tables " +
                        "WHERE table_schema = '" + view.getSchemaName() + "' and table_name = '" + view.getObjectName() + "'",
                "VALUES ('" + view.getObjectName() + "', 'BASE TABLE')");

        // system.jdbc.tables without filter
        assertThat(query("SELECT table_schem, table_name, table_type FROM system.jdbc.tables"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + view.getSchemaName() + "', '" + view.getObjectName() + "', 'TABLE')");

        // system.jdbc.tables with table prefix filter
        assertQuery(
                "SELECT table_schem, table_name, table_type " +
                        "FROM system.jdbc.tables " +
                        "WHERE table_cat = '" + view.getCatalogName() + "' AND " +
                        "table_schem = '" + view.getSchemaName() + "' AND " +
                        "table_name = '" + view.getObjectName() + "'",
                "VALUES ('" + view.getSchemaName() + "', '" + view.getObjectName() + "', 'TABLE')");

        // column listing
        assertThat(query("SHOW COLUMNS FROM " + view.getObjectName()))
                .projected(0) // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'nationkey', 'name', 'regionkey', 'comment'");

        assertThat(query("DESCRIBE " + view.getObjectName()))
                .projected(0) // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'nationkey', 'name', 'regionkey', 'comment'");

        // information_schema.columns without table_name filter
        assertThat(query(
                "SELECT table_name, column_name " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = '" + view.getSchemaName() + "'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES '" + view.getObjectName() + "') " +
                                "CROSS JOIN UNNEST(ARRAY['nationkey', 'name', 'regionkey', 'comment'])");

        // information_schema.columns with table_name filter
        assertThat(query(
                "SELECT table_name, column_name " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = '" + view.getSchemaName() + "' and table_name = '" + view.getObjectName() + "'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES '" + view.getObjectName() + "') " +
                                "CROSS JOIN UNNEST(ARRAY['nationkey', 'name', 'regionkey', 'comment'])");

        // view-specific listings
        checkInformationSchemaViewsForMaterializedView(view.getSchemaName(), view.getObjectName());

        // system.jdbc.columns without filter
        assertThat(query("SELECT table_schem, table_name, column_name FROM system.jdbc.columns"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + view.getSchemaName() + "', '" + view.getObjectName() + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['nationkey', 'name', 'regionkey', 'comment'])");

        // system.jdbc.columns with schema filter
        assertThat(query(
                "SELECT table_schem, table_name, column_name " +
                        "FROM system.jdbc.columns " +
                        "WHERE table_schem LIKE '%" + view.getSchemaName() + "%'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + view.getSchemaName() + "', '" + view.getObjectName() + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['nationkey', 'name', 'regionkey', 'comment'])");

        // system.jdbc.columns with table filter
        assertThat(query(
                "SELECT table_schem, table_name, column_name " +
                        "FROM system.jdbc.columns " +
                        "WHERE table_name LIKE '%" + view.getObjectName() + "%'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + view.getSchemaName() + "', '" + view.getObjectName() + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['nationkey', 'name', 'regionkey', 'comment'])");

        // details
        assertThat(((String) computeScalar("SHOW CREATE MATERIALIZED VIEW " + view.getObjectName())))
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
        assertThat(query(listMaterializedViewsSql("catalog_name = '" + view.getCatalogName() + "'")))
                .skippingTypesCheck()
                .containsAll(getTestingMaterializedViewsResultRows(view, otherView));

        assertThat(query(
                listMaterializedViewsSql(
                        "catalog_name = '" + otherView.getCatalogName() + "'",
                        "schema_name = '" + otherView.getSchemaName() + "'")))
                .skippingTypesCheck()
                .containsAll(getTestingMaterializedViewsResultRow(otherView, "sarcastic comment"));

        assertThat(query(
                listMaterializedViewsSql(
                        "catalog_name = '" + view.getCatalogName() + "'",
                        "schema_name = '" + view.getSchemaName() + "'",
                        "name = '" + view.getObjectName() + "'")))
                .skippingTypesCheck()
                .containsAll(getTestingMaterializedViewsResultRow(view, ""));

        assertThat(query(
                listMaterializedViewsSql("schema_name LIKE '%" + view.getSchemaName() + "%'")))
                .skippingTypesCheck()
                .containsAll(getTestingMaterializedViewsResultRow(view, ""));

        assertThat(query(
                listMaterializedViewsSql("name LIKE '%" + view.getObjectName() + "%'")))
                .skippingTypesCheck()
                .containsAll(getTestingMaterializedViewsResultRow(view, ""));

        // verify write in transaction
        if (!hasBehavior(SUPPORTS_MULTI_STATEMENT_WRITES)) {
            assertThatThrownBy(() -> inTransaction(session -> computeActual(session, "REFRESH MATERIALIZED VIEW " + view)))
                    .hasMessageMatching("Catalog only supports writes using autocommit: \\w+");
        }

        assertUpdate("DROP MATERIALIZED VIEW " + view);
        assertUpdate("DROP MATERIALIZED VIEW " + otherView);

        assertQueryReturnsEmptyResult(listMaterializedViewsSql("name = '" + view.getObjectName() + "'"));
        assertQueryReturnsEmptyResult(listMaterializedViewsSql("name = '" + otherView.getObjectName() + "'"));
        assertQueryReturnsEmptyResult(listMaterializedViewsSql("name = '" + viewWithComment.getObjectName() + "'"));
    }

    @Test
    public void testCompatibleTypeChangeForView()
    {
        skipTestUnless(supportsViews());

        String tableName = "test_table_" + randomTableSuffix();
        String viewName = "test_view_" + randomTableSuffix();

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
        skipTestUnless(supportsViews());

        String tableName = "test_table_" + randomTableSuffix();
        String viewName = "test_view_" + randomTableSuffix();

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

    @Test(dataProvider = "testViewMetadataDataProvider")
    public void testViewMetadata(String securityClauseInCreate, String securityClauseInShowCreate)
    {
        skipTestUnless(supportsViews());

        String viewName = "meta_test_view_" + randomTableSuffix();

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
        actual = computeActual("SHOW COLUMNS FROM " + viewName);

        expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("x", "bigint", "", "")
                .row("y", "varchar(3)", "", "")
                .build();

        assertEquals(actual, expected);

        // test SHOW CREATE VIEW
        String expectedSql = formatSqlText(format(
                "CREATE VIEW %s.%s.%s SECURITY %s AS %s",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                viewName,
                securityClauseInShowCreate,
                query)).trim();

        actual = computeActual("SHOW CREATE VIEW " + viewName);

        assertEquals(getOnlyElement(actual.getOnlyColumnAsSet()), expectedSql);

        actual = computeActual(format("SHOW CREATE VIEW %s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), viewName));

        assertEquals(getOnlyElement(actual.getOnlyColumnAsSet()), expectedSql);

        assertUpdate("DROP VIEW " + viewName);
    }

    @DataProvider
    public static Object[][] testViewMetadataDataProvider()
    {
        return new Object[][] {
                {"", "DEFINER"},
                {" SECURITY DEFINER", "DEFINER"},
                {" SECURITY INVOKER", "INVOKER"},
        };
    }

    @Test
    public void testShowCreateView()
    {
        skipTestUnless(supportsViews());
        checkState(getSession().getCatalog().isPresent(), "catalog is not set");
        checkState(getSession().getSchema().isPresent(), "schema is not set");

        String viewName = "test_show_create_view" + randomTableSuffix();
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

        assertEquals(computeActual("SHOW CREATE VIEW " + viewName).getOnlyValue(), ddl);

        assertUpdate("DROP VIEW " + viewName);
    }

    @Test
    public void testRenameMaterializedView()
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW));

        String schema = "rename_mv_test";
        Session session = Session.builder(getSession())
                .setSchema(schema)
                .build();

        QualifiedObjectName originalMaterializedView = new QualifiedObjectName(
                session.getCatalog().orElseThrow(),
                session.getSchema().orElseThrow(),
                "test_materialized_view_rename_" + randomTableSuffix());

        createTestingMaterializedView(originalMaterializedView, Optional.empty());

        String renamedMaterializedView = "test_materialized_view_rename_new_" + randomTableSuffix();
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
                format("VALUES ('%s', '%s')", originalMaterializedView.getCatalogName(), originalMaterializedView.getSchemaName()));
        assertQueryReturnsEmptyResult(session, listMaterializedViewsSql("name = '" + originalMaterializedView.getObjectName() + "'"));

        // rename with IF EXISTS on existing materialized view
        String testExistsMaterializedViewName = "test_materialized_view_rename_exists_" + randomTableSuffix();
        assertUpdate(session, "ALTER MATERIALIZED VIEW IF EXISTS " + renamedMaterializedView + " RENAME TO " + testExistsMaterializedViewName);
        assertTestingMaterializedViewQuery(schema, testExistsMaterializedViewName);

        // rename with upper-case, not delimited identifier
        String uppercaseName = "TEST_MATERIALIZED_VIEW_RENAME_UPPERCASE_" + randomTableSuffix();
        assertUpdate(session, "ALTER MATERIALIZED VIEW " + testExistsMaterializedViewName + " RENAME TO " + uppercaseName);
        assertTestingMaterializedViewQuery(schema, uppercaseName.toLowerCase(ENGLISH)); // Ensure select allows for lower-case, not delimited identifier

        String otherSchema = "rename_mv_other_schema";
        assertUpdate(format("CREATE SCHEMA IF NOT EXISTS %s", otherSchema));
        if (hasBehavior(SUPPORTS_RENAME_MATERIALIZED_VIEW_ACROSS_SCHEMAS)) {
            assertUpdate(session, "ALTER MATERIALIZED VIEW " + uppercaseName + " RENAME TO " + otherSchema + "." + originalMaterializedView.getObjectName());
            assertTestingMaterializedViewQuery(otherSchema, originalMaterializedView.getObjectName());

            assertUpdate(session, "DROP MATERIALIZED VIEW " + otherSchema + "." + originalMaterializedView.getObjectName());
        }
        else {
            assertQueryFails(
                    session,
                    "ALTER MATERIALIZED VIEW " + uppercaseName + " RENAME TO " + otherSchema + "." + originalMaterializedView.getObjectName(),
                    "Materialized View rename across schemas is not supported");
            assertUpdate(session, "DROP MATERIALIZED VIEW " + uppercaseName);
        }

        assertFalse(getQueryRunner().tableExists(session, originalMaterializedView.toString()));
        assertFalse(getQueryRunner().tableExists(session, renamedMaterializedView));
        assertFalse(getQueryRunner().tableExists(session, testExistsMaterializedViewName));

        // rename with IF EXISTS on NOT existing materialized view
        assertUpdate(session, "ALTER TABLE IF EXISTS " + originalMaterializedView + " RENAME TO " + renamedMaterializedView);
        assertQueryReturnsEmptyResult(session, listMaterializedViewsSql("name = '" + originalMaterializedView.getObjectName() + "'"));
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
        assertUpdate(format("CREATE SCHEMA IF NOT EXISTS %s", view.getSchemaName()));
        assertUpdate(format(
                "CREATE MATERIALIZED VIEW %s %s AS SELECT * FROM nation",
                view,
                comment.map(c -> format("COMMENT '%s'", c)).orElse("")));
    }

    private String getTestingMaterializedViewsResultRow(QualifiedObjectName materializedView, String comment)
    {
        return format(
                "VALUES ('%s', '%s', '%s', '%s', 'SELECT *\nFROM\n  nation\n')",
                materializedView.getCatalogName(),
                materializedView.getSchemaName(),
                materializedView.getObjectName(),
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
                materializedView.getCatalogName(),
                materializedView.getSchemaName(),
                materializedView.getObjectName(),
                viewDefinitionSql,
                otherMaterializedView.getCatalogName(),
                otherMaterializedView.getSchemaName(),
                otherMaterializedView.getObjectName(),
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

        String schemaName = getSession().getSchema().orElseThrow();

        String regularViewName = "test_views_together_normal_" + randomTableSuffix();
        assertUpdate("CREATE VIEW " + regularViewName + " AS SELECT * FROM region");

        String materializedViewName = "test_views_together_materialized_" + randomTableSuffix();
        assertUpdate("CREATE MATERIALIZED VIEW " + materializedViewName + " AS SELECT * FROM nation");

        // both should be accessible via information_schema.views
        // TODO: actually it is not the cased now hence overridable `checkInformationSchemaViewsForMaterializedView`
        assertThat(query("SELECT table_name FROM information_schema.views WHERE table_schema = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + regularViewName + "'");
        checkInformationSchemaViewsForMaterializedView(schemaName, materializedViewName);

        // check we can query from both
        assertThat(query("SELECT * FROM " + regularViewName)).containsAll("SELECT * FROM region");
        assertThat(query("SELECT * FROM " + materializedViewName)).containsAll("SELECT * FROM nation");

        assertUpdate("DROP VIEW " + regularViewName);
        assertUpdate("DROP MATERIALIZED VIEW " + materializedViewName);
    }

    // TODO inline when all implementations fixed
    protected void checkInformationSchemaViewsForMaterializedView(String schemaName, String viewName)
    {
        assertThat(query("SELECT table_name FROM information_schema.views WHERE table_schema = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + viewName + "'");
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
        assertEquals(emptySample.getMaterializedRows().size(), 0);
        assertTrue(all.getMaterializedRows().size() >= randomSample.getMaterializedRows().size());
    }

    @Test
    public void testTableSampleWithFiltering()
    {
        MaterializedResult emptySample = computeActual("SELECT DISTINCT orderkey, orderdate FROM orders TABLESAMPLE SYSTEM (99) WHERE orderkey BETWEEN 0 AND 0");
        MaterializedResult halfSample = computeActual("SELECT DISTINCT orderkey, orderdate FROM orders TABLESAMPLE SYSTEM (50) WHERE orderkey BETWEEN 0 AND 9999999999");
        MaterializedResult all = computeActual("SELECT orderkey, orderdate FROM orders");

        assertEquals(emptySample.getMaterializedRows().size(), 0);
        // Assertions need to be loose here because SYSTEM sampling random selects data on split boundaries. In this case either all the data will be selected, or
        // none of it. Sampling with a 100% ratio is ignored, so that also cannot be used to guarantee results.
        assertTrue(all.getMaterializedRows().size() >= halfSample.getMaterializedRows().size());
    }

    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                // If the connector reports additional column properties, the expected value needs to be adjusted in the test subclass
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders \\Q(\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")");
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
                        "('role_authorization_descriptors'), " +
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

        @Language("SQL") String ordersTableWithColumns = "VALUES " +
                "('orders', 'orderkey'), " +
                "('orders', 'custkey'), " +
                "('orders', 'orderstatus'), " +
                "('orders', 'totalprice'), " +
                "('orders', 'orderdate'), " +
                "('orders', 'orderpriority'), " +
                "('orders', 'clerk'), " +
                "('orders', 'shippriority'), " +
                "('orders', 'comment')";

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
                        "('role_authorization_descriptors'), " +
                        "('roles'), " +
                        "('schemata'), " +
                        "('table_privileges'), " +
                        "('tables'), " +
                        "('views')");
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

        String table = "test_rollback_" + randomTableSuffix();
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
                    format("ALTER SCHEMA %s RENAME TO %s", schemaName, schemaName + randomTableSuffix()),
                    "This connector does not support renaming schemas");
            return;
        }

        if (!hasBehavior(SUPPORTS_CREATE_SCHEMA)) {
            throw new SkipException("Skipping as connector does not support CREATE SCHEMA");
        }

        String schemaName = "test_rename_schema_" + randomTableSuffix();
        try {
            assertUpdate("CREATE SCHEMA " + schemaName);
            assertUpdate("ALTER SCHEMA " + schemaName + " RENAME TO " + schemaName + "_renamed");
        }
        finally {
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName + "_renamed");
        }
    }

    @Test
    public void testRenameTableAcrossSchema()
    {
        if (!hasBehavior(SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS)) {
            if (!hasBehavior(SUPPORTS_RENAME_TABLE)) {
                throw new SkipException("Skipping since rename table is not supported at all");
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

        String tableName = "test_rename_old_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String schemaName = "test_schema_" + randomTableSuffix();
        assertUpdate("CREATE SCHEMA " + schemaName);

        String renamedTable = schemaName + ".test_rename_new_" + randomTableSuffix();
        assertUpdate("ALTER TABLE " + tableName + " RENAME TO " + renamedTable);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertQuery("SELECT x FROM " + renamedTable, "VALUES 123");

        assertUpdate("DROP TABLE " + renamedTable);
        assertUpdate("DROP SCHEMA " + schemaName);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertFalse(getQueryRunner().tableExists(getSession(), renamedTable));
    }

    @Test
    public void testAddColumn()
    {
        if (!hasBehavior(SUPPORTS_ADD_COLUMN)) {
            assertQueryFails("ALTER TABLE nation ADD COLUMN test_add_column bigint", "This connector does not support adding columns");
            return;
        }

        skipTestUnless(supportsCreateTable());

        String tableName;
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_column_", "AS SELECT VARCHAR 'first' x")) {
            tableName = table.getName();
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN x bigint", ".* Column 'x' already exists");
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN X bigint", ".* Column 'X' already exists");
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN q bad_type", ".* Unknown type 'bad_type' for column 'q'");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN a varchar(50)");
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

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN x bigint");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN IF NOT EXISTS x bigint");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testDropColumn()
    {
        if (!hasBehavior(SUPPORTS_DROP_COLUMN)) {
            assertQueryFails("ALTER TABLE nation DROP COLUMN nationkey", "This connector does not support dropping columns");
            return;
        }

        skipTestUnless(supportsCreateTable());

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

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN notExistColumn");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testRenameColumn()
    {
        if (!hasBehavior(SUPPORTS_RENAME_COLUMN)) {
            assertQueryFails("ALTER TABLE nation RENAME COLUMN nationkey TO test_rename_column", "This connector does not support renaming columns");
            return;
        }

        skipTestUnless(supportsCreateTable());

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

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " RENAME COLUMN columnNotExists TO y");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " RENAME COLUMN IF EXISTS columnNotExists TO y");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testCreateTable()
    {
        String tableName = "test_create_" + randomTableSuffix();
        if (!supportsCreateTable()) {
            assertQueryFails("CREATE TABLE " + tableName + " (a bigint, b double, c varchar(50))", "This connector does not support creating tables");
            return;
        }

        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double, c varchar(50))");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        assertQueryFails("CREATE TABLE " + tableName + " (a bad_type)", ".* Unknown type 'bad_type' for column 'a'");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        // TODO (https://github.com/trinodb/trino/issues/5901) revert to longer name when Oracle version is updated
        tableName = "test_cr_not_exists_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b varchar(50), c double)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (d bigint, e varchar(50))");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        // Test CREATE TABLE LIKE
        tableName = "test_create_orig_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double, c varchar(50))");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        String tableNameLike = "test_create_like_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableNameLike + " (LIKE " + tableName + ", d bigint, e varchar(50))");
        assertTrue(getQueryRunner().tableExists(getSession(), tableNameLike));
        assertTableColumnNames(tableNameLike, "a", "b", "c", "d", "e");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        assertUpdate("DROP TABLE " + tableNameLike);
        assertFalse(getQueryRunner().tableExists(getSession(), tableNameLike));
    }

    @Test
    public void testCreateTableAsSelect()
    {
        String tableName = "test_ctas" + randomTableSuffix();
        if (!supportsCreateTable()) {
            assertQueryFails("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name, regionkey FROM nation", "This connector does not support creating tables with data");
            return;
        }
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertTableColumnNames(tableName, "name", "regionkey");
        assertUpdate("DROP TABLE " + tableName);

        // Some connectors support CREATE TABLE AS but not the ordinary CREATE TABLE. Let's test CTAS IF NOT EXISTS with a table that is guaranteed to exist.
        assertUpdate("CREATE TABLE IF NOT EXISTS nation AS SELECT custkey, acctbal FROM customer", 0);
        assertTableColumnNames("nation", "nationkey", "name", "regionkey", "comment");

        assertCreateTableAsSelect(
                "SELECT custkey, address, acctbal FROM customer",
                "SELECT count(*) FROM customer");

        assertCreateTableAsSelect(
                "SELECT mktsegment, sum(acctbal) x FROM customer GROUP BY mktsegment",
                "SELECT count(DISTINCT mktsegment) FROM customer");

        assertCreateTableAsSelect(
                "SELECT count(*) x FROM customer JOIN nation ON customer.nationkey = nation.nationkey",
                "SELECT 1");

        assertCreateTableAsSelect(
                "SELECT custkey FROM customer ORDER BY custkey LIMIT 10",
                "SELECT 10");

        assertCreateTableAsSelect(
                "SELECT * FROM customer WITH DATA",
                "SELECT * FROM customer",
                "SELECT count(*) FROM customer");

        assertCreateTableAsSelect(
                "SELECT * FROM customer WITH NO DATA",
                "SELECT * FROM customer LIMIT 0",
                "SELECT 0");

        // Tests for CREATE TABLE with UNION ALL: exercises PushTableWriteThroughUnion optimizer

        assertCreateTableAsSelect(
                "SELECT name, custkey, acctbal FROM customer WHERE custkey % 2 = 0 UNION ALL " +
                        "SELECT name, custkey, acctbal FROM customer WHERE custkey % 2 = 1",
                "SELECT name, custkey, acctbal FROM customer",
                "SELECT count(*) FROM customer");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "true").build(),
                "SELECT CAST(custkey AS BIGINT) custkey, acctbal FROM customer UNION ALL " +
                        "SELECT 1234567890, 1.23",
                "SELECT custkey, acctbal FROM customer UNION ALL " +
                        "SELECT 1234567890, 1.23",
                "SELECT count(*) + 1 FROM customer");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "SELECT CAST(custkey AS BIGINT) custkey, acctbal FROM customer UNION ALL " +
                        "SELECT 1234567890, 1.23",
                "SELECT custkey, acctbal FROM customer UNION ALL " +
                        "SELECT 1234567890, 1.23",
                "SELECT count(*) + 1 FROM customer");

        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE " + tableName + " AS SELECT mktsegment FROM customer");
        assertQuery("SELECT * from " + tableName, "SELECT mktsegment FROM customer");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableAsSelectWithUnicode()
    {
        // Covered by testCreateTableAsSelect
        skipTestUnless(supportsCreateTable());
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
        String table = "test_ctas_" + randomTableSuffix();
        assertUpdate(session, "CREATE TABLE " + table + " AS " + query, rowCountQuery);
        assertQuery(session, "SELECT * FROM " + table, expectedQuery);
        assertUpdate(session, "DROP TABLE " + table);

        assertFalse(getQueryRunner().tableExists(session, table));
    }

    @Test
    public void testCreateTableAsSelectNegativeDate()
    {
        String tableName = "negative_date_" + randomTableSuffix();

        if (!hasBehavior(SUPPORTS_CREATE_TABLE_WITH_DATA)) {
            assertQueryFails(format("CREATE TABLE %s AS SELECT DATE '-0001-01-01' AS dt", tableName), "This connector does not support creating tables with data");
            return;
        }
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
    {
        skipTestUnless(supportsCreateTable());
        String tableName = "test_rename_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String renamedTable = "test_rename_new_" + randomTableSuffix();
        if (!supportsRenameTable()) {
            assertQueryFails("ALTER TABLE " + tableName + " RENAME TO " + renamedTable, "This connector does not support renaming tables");
            return;
        }

        assertUpdate("ALTER TABLE " + tableName + " RENAME TO " + renamedTable);
        assertQuery("SELECT x FROM " + renamedTable, "VALUES 123");

        String testExistsTableName = "test_rename_exists_" + randomTableSuffix();
        assertUpdate("ALTER TABLE IF EXISTS " + renamedTable + " RENAME TO " + testExistsTableName);
        assertQuery("SELECT x FROM " + testExistsTableName, "VALUES 123");

        String uppercaseName = "TEST_RENAME_" + randomTableSuffix(); // Test an upper-case, not delimited identifier
        assertUpdate("ALTER TABLE " + testExistsTableName + " RENAME TO " + uppercaseName);
        assertQuery(
                "SELECT x FROM " + uppercaseName.toLowerCase(ENGLISH), // Ensure select allows for lower-case, not delimited identifier
                "VALUES 123");

        assertUpdate("DROP TABLE " + uppercaseName);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertFalse(getQueryRunner().tableExists(getSession(), renamedTable));

        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " RENAME TO " + renamedTable);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertFalse(getQueryRunner().tableExists(getSession(), renamedTable));
    }

    @Test
    public void testCommentTable()
    {
        if (!supportsCommentOnTable()) {
            assertQueryFails("COMMENT ON TABLE nation IS 'new comment'", "This connector does not support setting table comments");
            return;
        }

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_", "(a integer)")) {
            // comment set
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS 'new comment'");
            assertThat((String) computeActual("SHOW CREATE TABLE " + table.getName()).getOnlyValue()).contains("COMMENT 'new comment'");
            assertThat(getTableComment(table.getName())).isEqualTo("new comment");
            assertThat(query(
                    "SELECT table_name, comment FROM system.metadata.table_comments " +
                            "WHERE catalog_name = '" + catalogName + "' AND " +
                            "schema_name = '" + schemaName + "'"))
                    .skippingTypesCheck()
                    .containsAll("VALUES ('" + table.getName() + "', 'new comment')");

            // comment updated
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS 'updated comment'");
            assertThat(getTableComment(table.getName())).isEqualTo("updated comment");

            // comment set to empty or deleted
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS ''");
            assertThat(getTableComment(table.getName())).isIn("", null); // Some storages do not preserve empty comment

            // comment deleted
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS 'a comment'");
            assertThat(getTableComment(table.getName())).isEqualTo("a comment");
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS NULL");
            assertThat(getTableComment(table.getName())).isEqualTo(null);
        }

        String tableName = "test_comment_" + randomTableSuffix();
        try {
            // comment set when creating a table
            assertUpdate("CREATE TABLE " + tableName + "(key integer) COMMENT 'new table comment'");
            assertThat(getTableComment(tableName)).isEqualTo("new table comment");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private String getTableComment(String tableName)
    {
        // TODO use information_schema.tables.table_comment
        String result = (String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue();
        Matcher matcher = Pattern.compile("COMMENT '([^']*)'").matcher(result);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    @Test
    public void testCommentColumn()
    {
        if (!supportsCommentOnColumn()) {
            assertQueryFails("COMMENT ON COLUMN nation.nationkey IS 'new comment'", "This connector does not support setting column comments");
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_column_", "(a integer)")) {
            // comment set
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS 'new comment'");
            assertThat((String) computeActual("SHOW CREATE TABLE " + table.getName()).getOnlyValue()).contains("COMMENT 'new comment'");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("new comment");

            // comment updated
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS 'updated comment'");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("updated comment");

            // comment set to empty or deleted
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS ''");
            assertThat(getColumnComment(table.getName(), "a")).isIn("", null); // Some storages do not preserve empty comment

            // comment deleted
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS 'a comment'");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("a comment");
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS NULL");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo(null);
        }

        // TODO: comment set when creating a table
//        assertUpdate("CREATE TABLE " + tableName + "(a integer COMMENT 'new column comment')");
//        assertThat(getColumnComment(tableName, "a")).isEqualTo("new column comment");
//        assertUpdate("DROP TABLE " + tableName);
    }

    private String getColumnComment(String tableName, String columnName)
    {
        MaterializedResult materializedResult = computeActual(format(
                "SELECT comment FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'",
                getSession().getSchema().orElseThrow(),
                tableName,
                columnName));
        return (String) materializedResult.getOnlyValue();
    }

    @Test
    public void testInsert()
    {
        if (!supportsInsert()) {
            assertQueryFails("INSERT INTO nation(nationkey) VALUES (42)", "This connector does not support inserts");
            return;
        }

        String query = "SELECT phone, custkey, acctbal FROM customer";

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_", "AS " + query + " WITH NO DATA")) {
            assertQuery("SELECT count(*) FROM " + table.getName() + "", "SELECT 0");

            assertUpdate("INSERT INTO " + table.getName() + " " + query, "SELECT count(*) FROM customer");

            assertQuery("SELECT * FROM " + table.getName() + "", query);

            assertUpdate("INSERT INTO " + table.getName() + " (custkey) VALUES (-1)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (custkey) VALUES (null)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (phone) VALUES ('3283-2001-01-01')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (custkey, phone) VALUES (-2, '3283-2001-01-02')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (phone, custkey) VALUES ('3283-2001-01-03', -3)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (acctbal) VALUES (1234)", 1);

            assertQuery("SELECT * FROM " + table.getName() + "", query
                    + " UNION ALL SELECT null, -1, null"
                    + " UNION ALL SELECT null, null, null"
                    + " UNION ALL SELECT '3283-2001-01-01', null, null"
                    + " UNION ALL SELECT '3283-2001-01-02', -2, null"
                    + " UNION ALL SELECT '3283-2001-01-03', -3, null"
                    + " UNION ALL SELECT null, null, 1234");

            // UNION query produces columns in the opposite order
            // of how they are declared in the table schema
            assertUpdate(
                    "INSERT INTO " + table.getName() + " (custkey, phone, acctbal) " +
                            "SELECT custkey, phone, acctbal FROM customer " +
                            "UNION ALL " +
                            "SELECT custkey, phone, acctbal FROM customer",
                    "SELECT 2 * count(*) FROM customer");
        }
    }

    @Test
    public void testInsertForDefaultColumn()
    {
        skipTestUnless(supportsInsert());

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
        skipTestUnless(supportsInsert());

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
        skipTestUnless(supportsInsert());

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_unicode_", "(test varchar(50))")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801' ", 2);
            assertThat(computeActual("SELECT test FROM " + table.getName()).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("Hello", "hello测试􏿿world编码");
        }
    }

    @Test
    public void testInsertArray()
    {
        skipTestUnless(supportsInsert());

        String tableName = "test_insert_array_" + randomTableSuffix();
        if (!supportsArrays()) {
            assertThatThrownBy(() -> query("CREATE TABLE " + tableName + " (a array(bigint))"))
                    // TODO Unify failure message across connectors
                    .hasMessageMatching("[Uu]nsupported (column )?type: \\Qarray(bigint)");
            throw new SkipException("not supported");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_array_", "(a ARRAY<DOUBLE>, b ARRAY<BIGINT>)")) {
            assertUpdate("INSERT INTO " + table.getName() + " (a) VALUES (ARRAY[null])", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (a, b) VALUES (ARRAY[1.23E1], ARRAY[1.23E1])", 1);
            assertQuery("SELECT a[1], b[1] FROM " + table.getName(), "VALUES (null, null), (12.3, 12)");
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
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "commuted_not_null", "(nullable_col BIGINT, not_null_col BIGINT NOT NULL)")) {
            assertUpdate(format("INSERT INTO %s (not_null_col) VALUES (2)", table.getName()), 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES (NULL, 2)");
            // This is enforced by the engine and not the connector
            assertQueryFails(format("INSERT INTO %s (not_null_col, nullable_col) VALUES (NULL, 3)", table.getName()), "NULL value not allowed for NOT NULL column: not_null_col");
        }
    }

    @Language("RegExp")
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        throw new UnsupportedOperationException("This method should be overridden");
    }

    @Test
    public void testDelete()
    {
        skipTestUnlessSupportsDeletes();

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

        String tableName = "test_delete_" + randomTableSuffix();
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
    public void testDeleteWithComplexPredicate()
    {
        skipTestUnlessSupportsDeletes();

        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_complex_", "AS SELECT * FROM orders")) {
            // delete half the table, then delete the rest
            assertUpdate("DELETE FROM " + table.getName() + " WHERE orderkey % 2 = 0", "SELECT count(*) FROM orders WHERE orderkey % 2 = 0");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM orders WHERE orderkey % 2 <> 0");

            assertUpdate("DELETE FROM " + table.getName(), "SELECT count(*) FROM orders WHERE orderkey % 2 <> 0");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM orders LIMIT 0");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE rand() < 0", 0);
        }
    }

    @Test
    public void testDeleteWithSubquery()
    {
        skipTestUnlessSupportsDeletes();

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
    }

    @Test
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        skipTestUnlessSupportsDeletes();

        String tableName = "test_delete_" + randomTableSuffix();

        // delete using a subquery
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", 25);
        assertExplainAnalyze("EXPLAIN ANALYZE DELETE FROM " + tableName + " WHERE regionkey IN (SELECT regionkey FROM region WHERE name LIKE 'A%' LIMIT 1)",
                "SemiJoin.*");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDeleteWithSemiJoin()
    {
        skipTestUnlessSupportsDeletes();

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
        skipTestUnlessSupportsDeletes();

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_with_varchar_predicate_", "AS SELECT * FROM orders")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE orderstatus = 'O'", "SELECT count(*) FROM orders WHERE orderstatus = 'O'");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM orders WHERE orderstatus <> 'O'");
        }
    }

    protected void skipTestUnlessSupportsDeletes()
    {
        skipTestUnless(supportsCreateTable());
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_supports_delete", "(col varchar(1))")) {
            if (!supportsDelete()) {
                assertQueryFails("DELETE FROM " + table.getName(), "This connector does not support deletes");
                throw new SkipException("This connector does not support deletes");
            }
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
            assertQueryFails("DELETE FROM " + table.getName(), "This connector does not support deletes");
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
            assertQueryFails("DELETE FROM " + table.getName() + " WHERE regionkey = 2", "This connector does not support deletes");
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
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE) && hasBehavior(SUPPORTS_ROW_LEVEL_DELETE));
        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_row_delete", "AS SELECT * FROM region")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey = 2", 1);
            assertQuery("SELECT count(*) FROM " + table.getName(), "VALUES 4");
        }
    }

    @Test
    public void testUpdate()
    {
        if (!hasBehavior(SUPPORTS_UPDATE)) {
            // Note this change is a no-op, if actually run
            assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1", "This connector does not support updates");
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

    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @Test(timeOut = 60_000, invocationCount = 4)
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
                                if (trinoException != e && verifyFailure != e) {
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

    @Test
    public void testDropTableIfExists()
    {
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_if_exists"));
        assertUpdate("DROP TABLE IF EXISTS test_drop_if_exists");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_if_exists"));
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
        skipTestUnless(supportsCreateTable());

        QueryManager queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();
        executeExclusively(() -> {
            assertEventually(
                    new Duration(1, MINUTES),
                    () -> assertEquals(
                            queryManager.getQueries().stream()
                                    .map(BasicQueryInfo::getQueryId)
                                    .map(queryManager::getFullQueryInfo)
                                    .filter(info -> !info.isFinalQueryInfo())
                                    .collect(toList()),
                            ImmutableList.of()));

            // We cannot simply get the number of completed queries as soon as all the queries are completed, because this counter may not be up-to-date at that point.
            // The completed queries counter is updated in a final query info listener, which is called eventually.
            // Therefore, here we wait until the value of this counter gets stable.

            DispatchManager dispatchManager = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDispatchManager();
            long beforeCompletedQueriesCount = waitUntilStable(() -> dispatchManager.getStats().getCompletedQueries().getTotalCount(), new Duration(5, SECONDS));
            long beforeSubmittedQueriesCount = dispatchManager.getStats().getSubmittedQueries().getTotalCount();
            String tableName = "test_logging_count" + randomTableSuffix();
            assertUpdate("CREATE TABLE " + tableName + tableDefinitionForQueryLoggingCount());
            assertQueryReturnsEmptyResult("SELECT foo_1, foo_2_4 FROM " + tableName);
            assertUpdate("DROP TABLE " + tableName);
            assertQueryFails("SELECT * FROM " + tableName, ".*Table .* does not exist");

            // TODO: Figure out a better way of synchronization
            assertEventually(
                    new Duration(1, MINUTES),
                    () -> assertEquals(dispatchManager.getStats().getCompletedQueries().getTotalCount() - beforeCompletedQueriesCount, 4));
            assertEquals(dispatchManager.getStats().getSubmittedQueries().getTotalCount() - beforeSubmittedQueriesCount, 4);
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
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of(INFORMATION_SCHEMA, "tiny", "sf1")));
    }

    // TODO move to to engine-only
    @Test
    public void testSymbolAliasing()
    {
        skipTestUnless(supportsCreateTable());

        String tableName = "test_symbol_aliasing" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 foo_1, 2 foo_2_4", 1);
        assertQuery("SELECT foo_1, foo_2_4 FROM " + tableName, "SELECT 1, 2");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testWrittenStats()
    {
        skipTestUnless(supportsCreateTable());
        skipTestUnless(supportsInsert());

        String tableName = "test_written_stats_" + randomTableSuffix();
        try {
            String sql = "CREATE TABLE " + tableName + " AS SELECT * FROM nation";
            ResultWithQueryId<MaterializedResult> resultResultWithQueryId = getDistributedQueryRunner().executeWithQueryId(getSession(), sql);
            QueryInfo queryInfo = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());

            assertEquals(queryInfo.getQueryStats().getOutputPositions(), 1L);
            assertEquals(queryInfo.getQueryStats().getWrittenPositions(), 25L);
            assertTrue(queryInfo.getQueryStats().getLogicalWrittenDataSize().toBytes() > 0L);

            sql = "INSERT INTO " + tableName + " SELECT * FROM nation LIMIT 10";
            resultResultWithQueryId = getDistributedQueryRunner().executeWithQueryId(getSession(), sql);
            queryInfo = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());

            assertEquals(queryInfo.getQueryStats().getOutputPositions(), 1L);
            assertEquals(queryInfo.getQueryStats().getWrittenPositions(), 10L);
            assertTrue(queryInfo.getQueryStats().getLogicalWrittenDataSize().toBytes() > 0L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test(dataProvider = "testColumnNameDataProvider")
    public void testColumnName(String columnName)
    {
        skipTestUnless(supportsCreateTable());

        if (!requiresDelimiting(columnName)) {
            testColumnName(columnName, false);
        }
        testColumnName(columnName, true);
    }

    protected void testColumnName(String columnName, boolean delimited)
    {
        String nameInSql = columnName;
        if (delimited) {
            nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
        }
        String tableName = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "") + randomTableSuffix();

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

    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return false;
    }

    protected static boolean requiresDelimiting(String identifierName)
    {
        return !identifierName.matches("[a-zA-Z][a-zA-Z0-9_]*");
    }

    @DataProvider
    public Object[][] testColumnNameDataProvider()
    {
        return testColumnNameTestData().stream()
                .map(this::filterColumnNameTestData)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toDataProvider());
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
                .build();
    }

    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        return Optional.of(columnName);
    }

    protected String dataMappingTableName(String trinoTypeName)
    {
        return "test_data_mapping_smoke_" + trinoTypeName.replaceAll("[^a-zA-Z0-9]", "_") + "_" + randomTableSuffix();
    }

    @Test(dataProvider = "testDataMappingSmokeTestDataProvider")
    public void testDataMappingSmokeTest(DataMappingTestSetup dataMappingTestSetup)
    {
        testDataMapping(dataMappingTestSetup);
    }

    private void testDataMapping(DataMappingTestSetup dataMappingTestSetup)
    {
        skipTestUnless(supportsCreateTable());

        String trinoTypeName = dataMappingTestSetup.getTrinoTypeName();
        String sampleValueLiteral = dataMappingTestSetup.getSampleValueLiteral();
        String highValueLiteral = dataMappingTestSetup.getHighValueLiteral();

        String tableName = dataMappingTableName(trinoTypeName);

        Runnable setup = () -> {
            // TODO test with both CTAS *and* CREATE TABLE + INSERT, since they use different connector API methods.
            String createTable = "" +
                    "CREATE TABLE " + tableName + " AS " +
                    "SELECT CAST(row_id AS varchar(50)) row_id, CAST(value AS " + trinoTypeName + ") value " +
                    "FROM (VALUES " +
                    "  ('null value', NULL), " +
                    "  ('sample value', " + sampleValueLiteral + "), " +
                    "  ('high value', " + highValueLiteral + ")) " +
                    " t(row_id, value)";
            assertUpdate(createTable, 3);
        };
        if (dataMappingTestSetup.isUnsupportedType()) {
            String typeNameBase = trinoTypeName.replaceFirst("\\(.*", "");
            String expectedMessagePart = format("(%1$s.*not (yet )?supported)|((?i)unsupported.*%1$s)|((?i)not supported.*%1$s)", Pattern.quote(typeNameBase));
            assertThatThrownBy(setup::run)
                    .hasMessageFindingMatch(expectedMessagePart)
                    .satisfies(e -> assertThat(getTrinoExceptionCause(e)).hasMessageFindingMatch(expectedMessagePart));
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

        assertUpdate("DROP TABLE " + tableName);
    }

    @DataProvider
    public final Object[][] testDataMappingSmokeTestDataProvider()
    {
        return testDataMappingSmokeTestData().stream()
                .map(this::filterDataMappingSmokeTestData)
                .flatMap(Optional::stream)
                .collect(toDataProvider());
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
                .add(new DataMappingTestSetup("timestamp", "TIMESTAMP '2020-02-12 15:03:00'", "TIMESTAMP '2199-12-31 23:59:59.999'"))
                .add(new DataMappingTestSetup("timestamp(3) with time zone", "TIMESTAMP '2020-02-12 15:03:00 +01:00'", "TIMESTAMP '9999-12-31 23:59:59.999 +12:00'"))
                .add(new DataMappingTestSetup("char(3)", "'ab'", "'zzz'"))
                .add(new DataMappingTestSetup("varchar(3)", "'de'", "'zzz'"))
                .add(new DataMappingTestSetup("varchar", "'łąka for the win'", "'ŻŻŻŻŻŻŻŻŻŻ'"))
                .add(new DataMappingTestSetup("varbinary", "X'12ab3f'", "X'ffffffffffffffffffff'"))
                .build();
    }

    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        return Optional.of(dataMappingTestSetup);
    }

    @Test(dataProvider = "testCaseSensitiveDataMappingProvider")
    public void testCaseSensitiveDataMapping(DataMappingTestSetup dataMappingTestSetup)
    {
        testDataMapping(dataMappingTestSetup);
    }

    @DataProvider
    public final Object[][] testCaseSensitiveDataMappingProvider()
    {
        return testCaseSensitiveDataMappingData().stream()
                .map(this::filterCaseSensitiveDataMappingTestData)
                .flatMap(Optional::stream)
                .collect(toDataProvider());
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

    @Test(dataProvider = "testColumnNameDataProvider")
    public void testMaterializedViewColumnName(String columnName)
    {
        skipTestUnless(hasBehavior(SUPPORTS_CREATE_MATERIALIZED_VIEW));

        if (!requiresDelimiting(columnName)) {
            testMaterializedViewColumnName(columnName, false);
        }
        testMaterializedViewColumnName(columnName, true);
    }

    private void testMaterializedViewColumnName(String columnName, boolean delimited)
    {
        String nameInSql = columnName;
        if (delimited) {
            nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
        }
        String viewName = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "_") + "_" + randomTableSuffix();

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

    protected Consumer<Plan> assertPartialLimitWithPreSortedInputsCount(Session session, int expectedCount)
    {
        return plan -> {
            int actualCount = searchFrom(plan.getRoot())
                    .where(node -> node instanceof LimitNode && ((LimitNode) node).isPartial() && ((LimitNode) node).requiresPreSortedInputs())
                    .findAll()
                    .size();
            if (actualCount != expectedCount) {
                Metadata metadata = getDistributedQueryRunner().getCoordinator().getMetadata();
                String formattedPlan = textLogicalPlan(plan.getRoot(), plan.getTypes(), metadata, StatsAndCosts.empty(), session, 0, false);
                throw new AssertionError(format(
                        "Expected [\n%s\n] partial limit but found [\n%s\n] partial limit. Actual plan is [\n\n%s\n]",
                        expectedCount,
                        actualCount,
                        formattedPlan));
            }
        };
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
            // toString is brief because it's used for test case labels in IDE
            return trinoTypeName + (unsupportedType ? "!" : "");
        }
    }
}
