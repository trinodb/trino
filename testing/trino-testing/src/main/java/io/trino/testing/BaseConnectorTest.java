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

import io.trino.FeaturesConfig.JoinDistributionType;
import io.trino.Session;
import io.trino.cost.StatsAndCosts;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
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
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.trino.SystemSessionProperties.IGNORE_STATS_CALCULATOR_FAILURES;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.planprinter.PlanPrinter.textLogicalPlan;
import static io.trino.testing.DataProviders.toDataProvider;
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
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Generic test for connectors.
 *
 * @see AbstractTestDistributedQueries
 */
public abstract class BaseConnectorTest
        extends AbstractTestDistributedQueries
{
    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    @Override
    protected final boolean supportsCreateSchema()
    {
        return hasBehavior(SUPPORTS_CREATE_SCHEMA);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    @Override
    protected final boolean supportsCreateTable()
    {
        return hasBehavior(SUPPORTS_CREATE_TABLE);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    @Override
    protected final boolean supportsInsert()
    {
        return hasBehavior(SUPPORTS_INSERT);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    @Override
    protected final boolean supportsDelete()
    {
        return hasBehavior(SUPPORTS_DELETE);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    @Override
    protected final boolean supportsViews()
    {
        return hasBehavior(SUPPORTS_CREATE_VIEW);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    @Override
    protected final boolean supportsArrays()
    {
        return hasBehavior(SUPPORTS_ARRAY);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    @Override
    protected final boolean supportsCommentOnTable()
    {
        return hasBehavior(SUPPORTS_COMMENT_ON_TABLE);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    @Override
    protected final boolean supportsCommentOnColumn()
    {
        return hasBehavior(SUPPORTS_COMMENT_ON_COLUMN);
    }

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    @Override
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

    @Override
    public void testView()
    {
        // TODO merge AbstractTestDistributedQueries into BaseConnectorTest
        super.testView();
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
        @Language("SQL") String expectedValues = "VALUES ('" + view.getSchemaName() + "', '" + view.getObjectName() + "', 'nationkey'), " +
                "('" + view.getSchemaName() + "', '" + view.getObjectName() + "', 'name'), " +
                "('" + view.getSchemaName() + "', '" + view.getObjectName() + "', 'regionkey'), " +
                "('" + view.getSchemaName() + "', '" + view.getObjectName() + "', 'comment')";
        assertThat(query(
                "SELECT table_schem, table_name, column_name FROM system.jdbc.columns"))
                .skippingTypesCheck()
                .containsAll(expectedValues);

        // system.jdbc.columns with schema filter
        assertThat(query(
                "SELECT table_schem, table_name, column_name " +
                        "FROM system.jdbc.columns " +
                        "WHERE table_schem LIKE '%" + view.getSchemaName() + "%'"))
                .skippingTypesCheck()
                .containsAll(expectedValues);

        // system.jdbc.columns with table filter
        assertQuery(
                "SELECT table_schem, table_name, column_name " +
                        "FROM system.jdbc.columns " +
                        "WHERE table_name LIKE '%" + view.getObjectName() + "%'",
                expectedValues);

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

    @Override
    public void testAddColumn()
    {
        if (!hasBehavior(SUPPORTS_ADD_COLUMN)) {
            assertQueryFails("ALTER TABLE nation ADD COLUMN test_add_column bigint", "This connector does not support adding columns");
            return;
        }

        super.testAddColumn();
    }

    @Override
    public void testDropColumn()
    {
        if (!hasBehavior(SUPPORTS_DROP_COLUMN)) {
            assertQueryFails("ALTER TABLE nation DROP COLUMN nationkey", "This connector does not support dropping columns");
            return;
        }

        super.testDropColumn();
    }

    @Override
    public void testRenameColumn()
    {
        if (!hasBehavior(SUPPORTS_RENAME_COLUMN)) {
            assertQueryFails("ALTER TABLE nation RENAME COLUMN nationkey TO test_rename_column", "This connector does not support renaming columns");
            return;
        }

        super.testRenameColumn();
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
    public void verifySupportsDeleteDeclaration()
    {
        if (hasBehavior(SUPPORTS_DELETE)) {
            // Covered by testDeleteAllDataFromTable
            return;
        }

        skipTestUnless(hasBehavior(SUPPORTS_CREATE_TABLE));
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_supports_delete", "AS SELECT * FROM region")) {
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
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_supports_row_level_delete", "AS SELECT * FROM region")) {
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
}
