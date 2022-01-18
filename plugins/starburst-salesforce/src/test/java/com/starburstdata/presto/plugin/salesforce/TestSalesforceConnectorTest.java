/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.salesforce;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.units.Duration;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.server.BasicQueryInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.JoinCondition;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import io.trino.tpch.TpchTable;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.internal.collections.Pair;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.SystemSessionProperties.IGNORE_STATS_CALCULATOR_FAILURES;
import static io.trino.connector.informationschema.InformationSchemaTable.INFORMATION_SCHEMA;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.AGGREGATION_PUSHDOWN_ENABLED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_ARRAY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_COLUMN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_COMMENT_ON_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_VIEW;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_INSERT;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_RENAME_SCHEMA;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.lang.Thread.currentThread;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestSalesforceConnectorTest
        extends AbstractTestQueryFramework
{
    // This map is used for replacing tables and columns ending in __c with their non-__c counterpart when
    // running the expected queries against H2
    private static final Map<String, String> TABLE_COLUMN_SUFFIX_REGEXES;

    static {
        Map<String, String> tableRegexes = TpchTable.getTables().stream()
                .map(table -> Pair.of(table.getTableName() + "__c", table.getTableName()))
                .collect(Collectors.toMap(Pair::first, Pair::second));

        Map<String, String> columnRegexes = TpchTable.getTables().stream()
                .map(TpchTable::getColumns)
                .flatMap(List::stream)
                .map(column -> Pair.of(column.getSimplifiedColumnName() + "__c", column.getSimplifiedColumnName()))
                .distinct()
                .collect(Collectors.toMap(Pair::first, Pair::second));

        TABLE_COLUMN_SUFFIX_REGEXES = ImmutableMap.<String, String>builder()
                .putAll(tableRegexes)
                .putAll(columnRegexes)
                .build();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return SalesforceQueryRunner.builder().build();
    }

    // Override these assertions in order to change the expected SQL ran against H2
    // which does not contain the __c suffixes on the table and column names
    @Override
    protected void assertQuery(@Language("SQL") String sql)
    {
        @Language("SQL") String expectedSql = sql;
        for (Map.Entry<String, String> regex : TABLE_COLUMN_SUFFIX_REGEXES.entrySet()) {
            expectedSql = expectedSql.replaceAll(regex.getKey(), regex.getValue());
        }

        super.assertQuery(sql, expectedSql);
    }

    @Override
    protected void assertQuery(Session session, @Language("SQL") String sql)
    {
        @Language("SQL") String expectedSql = sql;
        for (Map.Entry<String, String> regex : TABLE_COLUMN_SUFFIX_REGEXES.entrySet()) {
            expectedSql = expectedSql.replaceAll(regex.getKey(), regex.getValue());
        }

        super.assertQuery(session, sql, expectedSql);
    }

    @Override
    protected void assertQueryOrdered(@Language("SQL") String sql)
    {
        @Language("SQL") String expectedSql = sql;
        for (Map.Entry<String, String> regex : TABLE_COLUMN_SUFFIX_REGEXES.entrySet()) {
            expectedSql = expectedSql.replaceAll(regex.getKey(), regex.getValue());
        }

        super.assertQueryOrdered(sql, expectedSql);
    }

    // Copied from QueryAssertions as it is package private
    static RuntimeException getTrinoExceptionCause(Throwable e)
    {
        return Throwables.getCausalChain(e).stream()
                .filter(TestSalesforceConnectorTest::isTrinoException)
                .findFirst() // TODO .collect(toOptional()) -- should be exactly one in the causal chain
                .map(RuntimeException.class::cast)
                .orElseThrow(() -> new IllegalArgumentException("Exception does not have TrinoException cause", e));
    }

    private static boolean isTrinoException(Throwable exception)
    {
        requireNonNull(exception, "exception is null");

        if (exception instanceof TrinoException || exception instanceof ParsingException) {
            return true;
        }

        if (exception.getClass().getName().equals("io.trino.client.FailureInfo$FailureException")) {
            try {
                String originalClassName = exception.toString().split(":", 2)[0];
                Class<? extends Throwable> originalClass = Class.forName(originalClassName).asSubclass(Throwable.class);
                return TrinoException.class.isAssignableFrom(originalClass) ||
                        ParsingException.class.isAssignableFrom(originalClass);
            }
            catch (ClassNotFoundException e) {
                return false;
            }
        }

        return false;
    }

    // AbstractTestQueries

    protected static final List<TpchTable<?>> REQUIRED_TPCH_TABLES = ImmutableList.of(CUSTOMER, NATION, ORDERS, REGION, LINE_ITEM);

    @Test
    public void testSelectLimitsTable()
    {
        // "current" value can change so just assert the limit
        assertQuery("SELECT type, limit FROM salesforce.system.limits WHERE type = 'API REQUESTS'", "SELECT 'API REQUESTS', 5000000");
    }

    @Test
    public void testAggregationOverUnknown()
    {
        assertQuery("SELECT clerk__c, min(totalprice__c), max(totalprice__c), min(nullvalue), max(nullvalue) " +
                "FROM (SELECT clerk__c, totalprice__c, null AS nullvalue FROM orders__c) " +
                "GROUP BY clerk__c");
    }

    @Test
    public void testLimitMax()
    {
        // max int
        assertQuery("SELECT orderkey__c FROM orders__c LIMIT " + Integer.MAX_VALUE);
        assertQuery("SELECT orderkey__c FROM orders__c ORDER BY orderkey__c LIMIT " + Integer.MAX_VALUE);

        // max long; a connector may attempt a pushdown while remote system may not accept such high limit values
        assertQuery("SELECT nationkey__c FROM nation__c LIMIT " + Long.MAX_VALUE, "SELECT nationkey FROM nation");
        // Currently this is not supported but once it's supported, it should be tested with connectors as well
        assertQueryFails("SELECT nationkey__c FROM nation__c ORDER BY nationkey__c LIMIT " + Long.MAX_VALUE, "ORDER BY LIMIT > 2147483647 is not supported");
    }

    @Test
    public void testComplexQuery()
    {
        assertQueryOrdered(
                "SELECT sum(orderkey__c), row_number() OVER (ORDER BY orderkey__c) " +
                        "FROM orders__c " +
                        "WHERE orderkey__c <= 10 " +
                        "GROUP BY orderkey__c " +
                        "HAVING sum(orderkey__c) >= 3 " +
                        "ORDER BY orderkey__c DESC " +
                        "LIMIT 3",
                "VALUES (7, 5), (6, 4), (5, 3)");
    }

    @Test
    public void testDistinctMultipleFields()
    {
        assertQuery("SELECT DISTINCT custkey__c, orderstatus__c FROM orders__c");
    }

    @Test
    public void testArithmeticNegation()
    {
        assertQuery("SELECT -custkey__c FROM orders__c");
    }

    @Test
    public void testDistinct()
    {
        assertQuery("SELECT DISTINCT custkey__c FROM orders__c");
    }

    @Test
    public void testDistinctHaving()
    {
        // Salesforce has a strict limit on the number of IDs it will support for aggregation pushdown
        // The assertion below on the orders table will fail, so we try a smaller table
        // See https://help.salesforce.com/articleView?id=000331769&type=1&mode=1
        assertQuery("SELECT COUNT(DISTINCT regionkey__c) AS count " +
                "FROM nation__c " +
                "GROUP BY name__c " +
                "HAVING COUNT(DISTINCT regionkey__c) > 1");

        // Then we'll disable aggregation pushdown and run the query
        checkState(getSession().getCatalog().isPresent());
        assertQuery(Session.builder(getSession())
                        .setCatalogSessionProperty(getSession().getCatalog().get(), AGGREGATION_PUSHDOWN_ENABLED, "false")
                        .build(),
                "SELECT COUNT(DISTINCT clerk__c) AS count " +
                        "FROM orders__c " +
                        "GROUP BY orderdate__c " +
                        "HAVING COUNT(DISTINCT clerk__c) > 1");
    }

    @Test
    public void testDistinctLimit()
    {
        assertQuery("" +
                "SELECT DISTINCT orderstatus__c, custkey__c " +
                "FROM (SELECT orderstatus__c, custkey__c FROM orders__c ORDER BY orderkey__c LIMIT 10) " +
                "LIMIT 10");
        assertQuery("SELECT COUNT(*) FROM (SELECT DISTINCT orderstatus__c, custkey__c FROM orders__c LIMIT 10)");
        assertQuery("SELECT DISTINCT custkey__c, orderstatus__c FROM orders__c WHERE custkey__c = 1268 LIMIT 2");

        assertQuery("" +
                        "SELECT DISTINCT x " +
                        "FROM (VALUES 1) t(x) JOIN (VALUES 10, 20) u(a) ON t.x < u.a " +
                        "LIMIT 100",
                "SELECT 1");
    }

    @Test
    public void testDistinctWithOrderBy()
    {
        assertQueryOrdered("SELECT DISTINCT custkey__c FROM orders__c ORDER BY custkey__c LIMIT 10");
    }

    @Test
    public void testRepeatedAggregations()
    {
        assertQuery("SELECT SUM(orderkey__c), SUM(orderkey__c) FROM orders__c");
    }

    @Test
    public void testLimit()
    {
        MaterializedResult actual = computeActual("SELECT orderkey__c FROM orders__c LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM orders", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testLimitWithAggregation()
    {
        MaterializedResult actual = computeActual("SELECT custkey__c, SUM(CAST(totalprice__c * 100 AS BIGINT)) FROM orders__c GROUP BY custkey__c LIMIT 10");
        MaterializedResult all = computeExpected("SELECT custkey, SUM(CAST(totalprice * 100 AS BIGINT)) FROM orders GROUP BY custkey", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testLimitInInlineView()
    {
        MaterializedResult actual = computeActual("SELECT orderkey__c FROM (SELECT orderkey__c FROM orders__c LIMIT 100) T LIMIT 10");
        MaterializedResult all = computeExpected("SELECT orderkey FROM orders", actual.getTypes());

        assertEquals(actual.getMaterializedRows().size(), 10);
        assertContains(all, actual);
    }

    @Test
    public void testCountAll()
    {
        assertQuery("SELECT COUNT(*) FROM orders__c");
        assertQuery("SELECT COUNT(42) FROM orders__c", "SELECT COUNT(*) FROM orders");
        assertQuery("SELECT COUNT(42 + 42) FROM orders__c", "SELECT COUNT(*) FROM orders");
        assertQuery("SELECT COUNT(null) FROM orders__c", "SELECT 0");
    }

    @Test
    public void testCountColumn()
    {
        assertQuery("SELECT COUNT(orderkey__c) FROM orders__c");
        assertQuery("SELECT COUNT(orderstatus__c) FROM orders__c");
        assertQuery("SELECT COUNT(orderdate__c) FROM orders__c");
        assertQuery("SELECT COUNT(1) FROM orders__c");

        assertQuery("SELECT COUNT(NULLIF(orderstatus__c, 'F')) FROM orders__c");
        assertQuery("SELECT COUNT(CAST(NULL AS BIGINT)) FROM orders__c"); // todo: make COUNT(null) work
    }

    @Test
    public void testSelectWithComparison()
    {
        assertQuery("SELECT orderkey__c FROM lineitem__c WHERE tax__c < discount__c");
    }

    @Test
    public void testIn()
    {
        assertQuery("SELECT orderkey__c FROM orders__c WHERE orderkey__c IN (1, 2, 3)");
        assertQuery("SELECT orderkey__c FROM orders__c WHERE orderkey__c IN (1.5, 2.3)", "SELECT orderkey FROM orders LIMIT 0"); // H2 incorrectly matches rows
        assertQuery("SELECT orderkey__c FROM orders__c WHERE orderkey__c IN (1, 2E0, 3)");
        assertQuery("SELECT orderkey__c FROM orders__c WHERE totalprice__c IN (1, 2, 3)");
    }

    @Test(dataProvider = "largeInValuesCount")
    public void testLargeIn(int valuesCount)
    {
        String longValues = range(0, valuesCount)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        assertQuery("SELECT orderkey__c FROM orders__c WHERE orderkey__c IN (" + longValues + ")");
        assertQuery("SELECT orderkey__c FROM orders__c WHERE orderkey__c NOT IN (" + longValues + ")");

        assertQuery("SELECT orderkey__c FROM orders__c WHERE orderkey__c IN (mod(1000, orderkey__c), " + longValues + ")");
        assertQuery("SELECT orderkey__c FROM orders__c WHERE orderkey__c NOT IN (mod(1000, orderkey__c), " + longValues + ")");
    }

    @DataProvider
    public static Object[][] largeInValuesCount()
    {
        return new Object[][] {
                {200},
                {500},
                {1000},
                {5000}
        };
    }

    @Test
    public void testShowSchemas()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS");
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of(getSession().getSchema().get(), INFORMATION_SCHEMA)));
    }

    @Test
    public void testShowSchemasFrom()
    {
        MaterializedResult result = computeActual(format("SHOW SCHEMAS FROM %s", getSession().getCatalog().get()));
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of(getSession().getSchema().get(), INFORMATION_SCHEMA)));
    }

    @Test
    public void testShowSchemasLike()
    {
        MaterializedResult result = computeActual(format("SHOW SCHEMAS LIKE '%s'", getSession().getSchema().get()));
        org.testng.Assert.assertEquals(result.getOnlyColumnAsSet(), ImmutableSet.of(getSession().getSchema().get()));
    }

    @Test
    public void testShowSchemasLikeWithEscape()
    {
        assertQueryFails("SHOW SCHEMAS LIKE 't$_%' ESCAPE ''", "Escape string must be a single character");
        assertQueryFails("SHOW SCHEMAS LIKE 't$_%' ESCAPE '$$'", "Escape string must be a single character");

        Set<Object> allSchemas = computeActual("SHOW SCHEMAS").getOnlyColumnAsSet();
        org.testng.Assert.assertEquals(allSchemas, computeActual("SHOW SCHEMAS LIKE '%_%'").getOnlyColumnAsSet());
        Set<Object> result = computeActual("SHOW SCHEMAS LIKE '%$_%' ESCAPE '$'").getOnlyColumnAsSet();
        assertNotEquals(allSchemas, result);
        assertThat(result).contains("information_schema").allMatch(schemaName -> ((String) schemaName).contains("_"));
    }

    @Test
    public void testShowTables()
    {
        Set<String> expectedTables = REQUIRED_TPCH_TABLES.stream()
                .map(table -> table.getTableName() + "__c")
                .collect(toImmutableSet());

        MaterializedResult result = computeActual("SHOW TABLES");
        assertThat(result.getOnlyColumnAsSet()).containsAll(expectedTables);
    }

    @Test
    public void testShowTablesLike()
    {
        assertThat(computeActual("SHOW TABLES LIKE 'or%'").getOnlyColumnAsSet())
                .contains("orders__c")
                .allMatch(tableName -> ((String) tableName).startsWith("or"));
    }

    @Test
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM orders__c");
        MaterializedResult expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("id", "varchar(18)", "", "Label Record ID corresponds to this field.")
                .row("ownerid", "varchar(18)", "", "Label Owner ID corresponds to this field.")
                .row("isdeleted", "boolean", "", "Label Deleted corresponds to this field.")
                .row("name", "varchar(80)", "", "Label Name corresponds to this field.")
                .row("createddate", "timestamp(0)", "", "Label Created Date corresponds to this field.")
                .row("createdbyid", "varchar(18)", "", "Label Created By ID corresponds to this field.")
                .row("lastmodifieddate", "timestamp(0)", "", "Label Last Modified Date corresponds to this field.")
                .row("lastmodifiedbyid", "varchar(18)", "", "Label Last Modified By ID corresponds to this field.")
                .row("systemmodstamp", "timestamp(0)", "", "Label System Modstamp corresponds to this field.")
                .row("lastactivitydate", "date", "", "Label Last Activity Date corresponds to this field.")
                .row("shippriority__c", "double", "", "Label shippriority corresponds to this field.")
                .row("custkey__c", "double", "", "Label custkey corresponds to this field.")
                .row("orderstatus__c", "varchar(1)", "", "Label orderstatus corresponds to this field.")
                .row("totalprice__c", "double", "", "Label totalprice corresponds to this field.")
                .row("orderkey__c", "double", "", "Label orderkey corresponds to this field.")
                .row("comment__c", "varchar(79)", "", "Label comment corresponds to this field.")
                .row("orderdate__c", "date", "", "Label orderdate corresponds to this field.")
                .row("orderpriority__c", "varchar(15)", "", "Label orderpriority corresponds to this field.")
                .row("clerk__c", "varchar(15)", "", "Label clerk corresponds to this field.")
                .build();

        // Until we migrate all connectors to parametrized varchar we check two options
        assertEquals(actual, expected);
    }

    @Test
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders__c' LIMIT 1",
                "SELECT 'orders__c' table_name");

        // Salesforce data type for custkey column is double
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'double' AND table_name = 'customer__c' and column_name = 'custkey__c' LIMIT 1",
                "SELECT 'customer__c' table_name");
    }

    @Test
    public void testInformationSchemaUppercaseName()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'LOCAL'",
                "SELECT '' WHERE false");
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'TINY'",
                "SELECT '' WHERE false");
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'ORDERS'",
                "SELECT '' WHERE false");
    }

    @Test
    public void testTopN()
    {
        assertQueryOrdered("SELECT n.name__c, r.name__c FROM nation__c n LEFT JOIN region__c r ON n.regionkey__c = r.regionkey__c ORDER BY n.name__c LIMIT 1");

        assertQueryOrdered("SELECT orderkey__c FROM orders__c ORDER BY orderkey__c LIMIT 10");
        assertQueryOrdered("SELECT orderkey__c FROM orders__c ORDER BY orderkey__c DESC LIMIT 10");

        // multiple sort columns with different sort orders__c
        assertQueryOrdered("SELECT orderpriority__c, totalprice__c FROM orders__c ORDER BY orderpriority__c DESC, totalprice__c ASC LIMIT 10");

        // TopN with Filter
        assertQueryOrdered("SELECT orderkey__c FROM orders__c WHERE orderkey__c > 10 ORDER BY orderkey__c DESC LIMIT 10");

        // TopN over aggregation column
        assertQueryOrdered("SELECT sum(totalprice__c), clerk__c FROM orders__c GROUP BY clerk__c ORDER BY sum(totalprice__c) LIMIT 10");

        // TopN over TopN
        assertQueryOrdered("SELECT orderkey__c, totalprice__c FROM (SELECT orderkey__c, totalprice__c FROM orders__c ORDER BY 1, 2 LIMIT 10) ORDER BY 2, 1 LIMIT 5");

        // TopN over complex query
        assertQueryOrdered(
                "SELECT totalprice__c_sum, clerk__c " +
                        "FROM (SELECT SUM(totalprice__c) as totalprice__c_sum, clerk__c FROM orders__c WHERE orderpriority__c='1-URGENT' GROUP BY clerk__c ORDER BY totalprice__c_sum DESC LIMIT 10)" +
                        "ORDER BY clerk__c DESC LIMIT 5");

        // TopN over aggregation with filter
        assertQueryOrdered(
                "SELECT * " +
                        "FROM (SELECT SUM(totalprice__c) as sum, custkey__c AS total FROM orders__c GROUP BY custkey__c HAVING COUNT(*) > 3) " +
                        "ORDER BY sum DESC LIMIT 10");
    }

    @Test
    public void testTopNByMultipleFields()
    {
        assertQueryOrdered("SELECT orderkey__c, custkey__c, orderstatus__c FROM orders__c ORDER BY orderkey__c ASC, custkey__c ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey__c, custkey__c, orderstatus__c FROM orders__c ORDER BY orderkey__c ASC, custkey__c DESC LIMIT 10");
        assertQueryOrdered("SELECT orderkey__c, custkey__c, orderstatus__c FROM orders__c ORDER BY orderkey__c DESC, custkey__c ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey__c, custkey__c, orderstatus__c FROM orders__c ORDER BY orderkey__c DESC, custkey__c DESC LIMIT 10");

        // now try with order by fields swapped
        assertQueryOrdered("SELECT orderkey__c, custkey__c, orderstatus__c FROM orders__c ORDER BY custkey__c ASC, orderkey__c ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey__c, custkey__c, orderstatus__c FROM orders__c ORDER BY custkey__c ASC, orderkey__c DESC LIMIT 10");
        assertQueryOrdered("SELECT orderkey__c, custkey__c, orderstatus__c FROM orders__c ORDER BY custkey__c DESC, orderkey__c ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey__c, custkey__c, orderstatus__c FROM orders__c ORDER BY custkey__c DESC, orderkey__c DESC LIMIT 10");

        // nulls first
        assertQueryOrdered("SELECT orderkey__c, custkey__c, orderstatus__c FROM orders__c ORDER BY nullif(orderkey__c, 3) ASC NULLS FIRST, custkey__c ASC LIMIT 10");
        assertQueryOrdered("SELECT orderkey__c, custkey__c, orderstatus__c FROM orders__c ORDER BY nullif(orderkey__c, 3) DESC NULLS FIRST, custkey__c ASC LIMIT 10");

        // nulls last
        assertQueryOrdered("SELECT orderkey__c, custkey__c, orderstatus__c FROM orders__c ORDER BY nullif(orderkey__c, 3) ASC NULLS LAST LIMIT 10");
        assertQueryOrdered("SELECT orderkey__c, custkey__c, orderstatus__c FROM orders__c ORDER BY nullif(orderkey__c, 3) DESC NULLS LAST, custkey__c ASC LIMIT 10");

        // assure that default is nulls last
        assertQueryOrdered(
                "SELECT orderkey__c, custkey__c, orderstatus__c FROM orders__c ORDER BY nullif(orderkey__c, 3) ASC, custkey__c ASC LIMIT 10",
                "SELECT orderkey, custkey, orderstatus FROM orders ORDER BY nullif(orderkey, 3) ASC NULLS LAST, custkey ASC LIMIT 10");
    }

    @Test
    public void testLimitPushDown()
    {
        // TODO Not sure what is going on here, getting an error from H2, thinks this date is a double type
        //   Caused by: org.h2.jdbc.JdbcSQLDataException: Data conversion error converting "1996-01-02" [22018-200]
        throw new SkipException("testLimitPushDown");
    }

    @Test
    public void testPredicate()
    {
        assertQuery("" +
                "SELECT *\n" +
                "FROM (\n" +
                "  SELECT orderkey__c+1 AS a FROM orders__c WHERE orderstatus__c = 'F' UNION ALL \n" +
                "  SELECT orderkey__c FROM orders__c WHERE orderkey__c % 2 = 0 UNION ALL \n" +
                "  (SELECT orderkey__c+custkey__c FROM orders__c ORDER BY orderkey__c LIMIT 10)\n" +
                ") \n" +
                "WHERE a < 20 OR a > 100 \n" +
                "ORDER BY a");
    }

    @Test
    public void testTableSampleBernoulliBoundaryValues()
    {
        MaterializedResult fullSample = computeActual("SELECT orderkey__c FROM orders__c TABLESAMPLE BERNOULLI (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey__c FROM orders__c TABLESAMPLE BERNOULLI (0)");
        MaterializedResult all = computeExpected("SELECT orderkey FROM orders", fullSample.getTypes());

        assertContains(all, fullSample);
        assertEquals(emptySample.getMaterializedRows().size(), 0);
    }

    @Test
    public void testTableSampleBernoulli()
    {
        DescriptiveStatistics stats = new DescriptiveStatistics();

        int total = computeExpected("SELECT orderkey FROM orders", ImmutableList.of(BIGINT)).getMaterializedRows().size();

        for (int i = 0; i < 100; i++) {
            List<MaterializedRow> values = computeActual("SELECT orderkey__c FROM orders__c TABLESAMPLE BERNOULLI (50)").getMaterializedRows();

            org.testng.Assert.assertEquals(values.size(), ImmutableSet.copyOf(values).size(), "TABLESAMPLE produced duplicate rows");
            stats.addValue(values.size() * 1.0 / total);
        }

        double mean = stats.getGeometricMean();
        assertTrue(mean > 0.45 && mean < 0.55, format("Expected mean sampling rate to be ~0.5, but was %s", mean));
    }

    @Test
    public void testFilterPushdownWithAggregation()
    {
        assertQuery("SELECT * FROM (SELECT count(*) FROM orders__c) WHERE 0=1");
        assertQuery("SELECT * FROM (SELECT count(*) FROM orders__c) WHERE null");
    }

    // AbstractTestDistributedQueries

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
    public void testCreateTable()
    {
        String tableName = "test_create_" + randomTableSuffix();
        if (!supportsCreateTable()) {
            assertQueryFails("CREATE TABLE " + tableName + " (a bigint, b double, c varchar)", "This connector does not support creating tables");
            return;
        }

        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double, c varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        assertQueryFails("CREATE TABLE " + tableName + " (a bad_type)", ".* Unknown type 'bad_type' for column 'a'");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        // TODO (https://github.com/trinodb/trino/issues/5901) revert to longer name__c when Oracle version is updated
        tableName = "test_cr_tab_not_exists_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b varchar, c double)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (d bigint, e varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        // Test CREATE TABLE LIKE
        tableName = "test_create_original_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double, c varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        String tableNameLike = "test_create_like_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableNameLike + " (LIKE " + tableName + ", d bigint, e varchar)");
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
            assertQueryFails("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name__c, regionkey__c FROM nation__c", "This connector does not support creating tables with data");
            return;
        }
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name__c, regionkey__c FROM nation__c", "SELECT count(*) FROM nation__c");
        assertTableColumnNames(tableName, "name__c", "regionkey__c");
        assertUpdate("DROP TABLE " + tableName);

        // Some connectors support CREATE TABLE AS but not the ordinary CREATE TABLE. Let's test CTAS IF NOT EXISTS with a table that is guaranteed to exist.
        assertUpdate("CREATE TABLE IF NOT EXISTS nation__c AS SELECT custkey__c, acctbal FROM customer__c", 0);
        assertTableColumnNames("nation__c", "nationkey__c", "name__c", "regionkey__c", "comment__c");

        assertCreateTableAsSelect(
                "SELECT custkey__c, address, acctbal FROM customer__c",
                "SELECT count(*) FROM customer__c");

        assertCreateTableAsSelect(
                "SELECT mktsegment, sum(acctbal) x FROM customer__c GROUP BY mktsegment",
                "SELECT count(DISTINCT mktsegment) FROM customer__c");

        assertCreateTableAsSelect(
                "SELECT count(*) x FROM customer__c JOIN nation__c ON customer__c.nationkey__c = nation__c.nationkey__c",
                "SELECT 1");

        assertCreateTableAsSelect(
                "SELECT custkey__c FROM customer__c ORDER BY custkey__c LIMIT 10",
                "SELECT 10");

        assertCreateTableAsSelect(
                "SELECT '\u2603' unicode",
                "SELECT 1");

        assertCreateTableAsSelect(
                "SELECT * FROM customer__c WITH DATA",
                "SELECT * FROM customer__c",
                "SELECT count(*) FROM customer__c");

        assertCreateTableAsSelect(
                "SELECT * FROM customer__c WITH NO DATA",
                "SELECT * FROM customer__c LIMIT 0",
                "SELECT 0");

        // Tests for CREATE TABLE with UNION ALL: exercises PushTableWriteThroughUnion optimizer

        assertCreateTableAsSelect(
                "SELECT name__c, custkey__c, acctbal FROM customer__c WHERE custkey__c % 2 = 0 UNION ALL " +
                        "SELECT name__c, custkey__c, acctbal FROM customer__c WHERE custkey__c % 2 = 1",
                "SELECT name__c, custkey__c, acctbal FROM customer__c",
                "SELECT count(*) FROM customer__c");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "true").build(),
                "SELECT CAST(custkey__c AS BIGINT) custkey__c, acctbal FROM customer__c UNION ALL " +
                        "SELECT 1234567890, 1.23",
                "SELECT custkey__c, acctbal FROM customer__c UNION ALL " +
                        "SELECT 1234567890, 1.23",
                "SELECT count(*) + 1 FROM customer__c");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "SELECT CAST(custkey__c AS BIGINT) custkey__c, acctbal FROM customer__c UNION ALL " +
                        "SELECT 1234567890, 1.23",
                "SELECT custkey__c, acctbal FROM customer__c UNION ALL " +
                        "SELECT 1234567890, 1.23",
                "SELECT count(*) + 1 FROM customer__c");

        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE " + tableName + " AS SELECT mktsegment FROM customer__c");
        assertQuery("SELECT * from " + tableName, "SELECT mktsegment FROM customer__c");
        assertUpdate("DROP TABLE " + tableName);
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
        String table = "test_table_" + randomTableSuffix();
        assertUpdate(session, "CREATE TABLE " + table + " AS " + query, rowCountQuery);
        assertQuery(session, "SELECT * FROM " + table, expectedQuery);
        assertUpdate(session, "DROP TABLE " + table);

        assertFalse(getQueryRunner().tableExists(session, table));
    }

    @Test
    public void testRenameTable()
    {
        skipTestUnless(supportsCreateTable());

        String tableName = "test_rename_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String renamedTable = "test_rename_new_" + randomTableSuffix();
        assertUpdate("ALTER TABLE " + tableName + " RENAME TO " + renamedTable);
        assertQuery("SELECT x FROM " + renamedTable, "VALUES 123");

        String testExistsTableName = "test_rename_new_exists_" + randomTableSuffix();
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
            assertQueryFails("COMMENT ON TABLE nation__c IS 'new comment__c'", "This connector does not support setting table comments");
            return;
        }

        String tableName = "test_comment_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(a integer)");

        // comment__c set
        assertUpdate("COMMENT ON TABLE " + tableName + " IS 'new comment__c'");
        assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue()).contains("COMMENT 'new comment__c'");
        assertThat(getTableComment(tableName)).isEqualTo("new comment__c");

        // comment__c updated
        assertUpdate("COMMENT ON TABLE " + tableName + " IS 'updated comment__c'");
        assertThat(getTableComment(tableName)).isEqualTo("updated comment__c");

        // comment__c set to empty or deleted
        assertUpdate("COMMENT ON TABLE " + tableName + " IS ''");
        assertThat(getTableComment(tableName)).isIn("", null); // Some storages do not preserve empty comment__c

        // comment__c deleted
        assertUpdate("COMMENT ON TABLE " + tableName + " IS 'a comment__c'");
        assertThat(getTableComment(tableName)).isEqualTo("a comment__c");
        assertUpdate("COMMENT ON TABLE " + tableName + " IS NULL");
        assertThat(getTableComment(tableName)).isEqualTo(null);

        assertUpdate("DROP TABLE " + tableName);

        // comment__c set when creating a table
        assertUpdate("CREATE TABLE " + tableName + "(key integer) COMMENT 'new table comment__c'");
        assertThat(getTableComment(tableName)).isEqualTo("new table comment__c");
        assertUpdate("DROP TABLE " + tableName);
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
            assertQueryFails("COMMENT ON COLUMN nation__c.nationkey__c IS 'new comment__c'", "This connector does not support setting column comments");
            return;
        }

        String tableName = "test_comment_column_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + "(a integer)");

        // comment__c set
        assertUpdate("COMMENT ON COLUMN " + tableName + ".a IS 'new comment__c'");
        assertThat((String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue()).contains("COMMENT 'new comment__c'");
        assertThat(getColumnComment(tableName, "a")).isEqualTo("new comment__c");

        // comment__c updated
        assertUpdate("COMMENT ON COLUMN " + tableName + ".a IS 'updated comment__c'");
        assertThat(getColumnComment(tableName, "a")).isEqualTo("updated comment__c");

        // comment__c set to empty or deleted
        assertUpdate("COMMENT ON COLUMN " + tableName + ".a IS ''");
        assertThat(getColumnComment(tableName, "a")).isIn("", null); // Some storages do not preserve empty comment__c

        // comment__c deleted
        assertUpdate("COMMENT ON COLUMN " + tableName + ".a IS 'a comment__c'");
        assertThat(getColumnComment(tableName, "a")).isEqualTo("a comment__c");
        assertUpdate("COMMENT ON COLUMN " + tableName + ".a IS NULL");
        assertThat(getColumnComment(tableName, "a")).isEqualTo(null);

        assertUpdate("DROP TABLE " + tableName);

        // TODO: comment__c set when creating a table
//        assertUpdate("CREATE TABLE " + tableName + "(a integer COMMENT 'new column comment__c')");
//        assertThat(getColumnComment(tableName, "a")).isEqualTo("new column comment__c");
//        assertUpdate("DROP TABLE " + tableName);
    }

    private String getColumnComment(String tableName, String columnName)
    {
        MaterializedResult materializedResult = computeActual(format(
                "SELECT comment__c FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'",
                getSession().getSchema().orElseThrow(),
                tableName,
                columnName));
        return (String) materializedResult.getOnlyValue();
    }

    @Test
    public void testRenameColumn()
    {
        skipTestUnless(supportsCreateTable());

        String tableName = "test_rename_column_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 'some value' x", 1);

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

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " RENAME COLUMN columnNotExists TO y");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " RENAME COLUMN IF EXISTS columnNotExists TO y");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testDropColumn()
    {
        skipTestUnless(supportsCreateTable());

        String tableName = "test_drop_column_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x, 456 y, 111 a", 1);

        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN x");
        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS y");
        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertQueryFails("SELECT x FROM " + tableName, ".* Column 'x' cannot be resolved");
        assertQueryFails("SELECT y FROM " + tableName, ".* Column 'y' cannot be resolved");

        assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN a", ".* Cannot drop the only column in a table");

        assertUpdate("DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN notExistColumn");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testAddColumn()
    {
        skipTestUnless(supportsCreateTable());

        String tableName = "test_add_column_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT CAST('first' AS varchar) x", 1);

        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN x bigint", ".* Column 'x' already exists");
        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN X bigint", ".* Column 'X' already exists");
        assertQueryFails("ALTER TABLE " + tableName + " ADD COLUMN q bad_type", ".* Unknown type 'bad_type' for column 'q'");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN a varchar");
        assertUpdate("INSERT INTO " + tableName + " SELECT 'second', 'xxx'", 1);
        assertQuery(
                "SELECT x, a FROM " + tableName,
                "VALUES ('first', NULL), ('second', 'xxx')");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN b double");
        assertUpdate("INSERT INTO " + tableName + " SELECT 'third', 'yyy', 33.3E0", 1);
        assertQuery(
                "SELECT x, a, b FROM " + tableName,
                "VALUES ('first', NULL, NULL), ('second', 'xxx', NULL), ('third', 'yyy', 33.3)");

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS c varchar");
        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN IF NOT EXISTS c varchar");
        assertUpdate("INSERT INTO " + tableName + " SELECT 'fourth', 'zzz', 55.3E0, 'newColumn'", 1);
        assertQuery(
                "SELECT x, a, b, c FROM " + tableName,
                "VALUES ('first', NULL, NULL, NULL), ('second', 'xxx', NULL, NULL), ('third', 'yyy', 33.3, NULL), ('fourth', 'zzz', 55.3, 'newColumn')");
        assertUpdate("DROP TABLE " + tableName);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN x bigint");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN IF NOT EXISTS x bigint");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsert()
    {
        if (!supportsInsert()) {
            assertQueryFails("INSERT INTO nation__c(nationkey__c) VALUES (42)", "This connector does not support inserts");
            return;
        }

        String query = "SELECT phone, custkey__c, acctbal FROM customer__c";

        String tableName = "test_insert_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS " + query + " WITH NO DATA", 0);
        assertQuery("SELECT count(*) FROM " + tableName + "", "SELECT 0");

        assertUpdate("INSERT INTO " + tableName + " " + query, "SELECT count(*) FROM customer__c");

        assertQuery("SELECT * FROM " + tableName + "", query);

        assertUpdate("INSERT INTO " + tableName + " (custkey__c) VALUES (-1)", 1);
        assertUpdate("INSERT INTO " + tableName + " (custkey__c) VALUES (null)", 1);
        assertUpdate("INSERT INTO " + tableName + " (phone) VALUES ('3283-2001-01-01')", 1);
        assertUpdate("INSERT INTO " + tableName + " (custkey__c, phone) VALUES (-2, '3283-2001-01-02')", 1);
        assertUpdate("INSERT INTO " + tableName + " (phone, custkey__c) VALUES ('3283-2001-01-03', -3)", 1);
        assertUpdate("INSERT INTO " + tableName + " (acctbal) VALUES (1234)", 1);

        assertQuery("SELECT * FROM " + tableName + "", query
                + " UNION ALL SELECT null, -1, null"
                + " UNION ALL SELECT null, null, null"
                + " UNION ALL SELECT '3283-2001-01-01', null, null"
                + " UNION ALL SELECT '3283-2001-01-02', -2, null"
                + " UNION ALL SELECT '3283-2001-01-03', -3, null"
                + " UNION ALL SELECT null, null, 1234");

        // UNION query produces columns in the opposite order
        // of how they are declared in the table schema
        assertUpdate(
                "INSERT INTO " + tableName + " (custkey__c, phone, acctbal) " +
                        "SELECT custkey__c, phone, acctbal FROM customer__c " +
                        "UNION ALL " +
                        "SELECT custkey__c, phone, acctbal FROM customer__c",
                "SELECT 2 * count(*) FROM customer__c");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testInsertUnicode()
    {
        skipTestUnless(supportsInsert());

        String tableName = "test_insert_unicode_" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName + "(test varchar)");
        assertUpdate("INSERT INTO " + tableName + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801' ", 2);
        assertThat(computeActual("SELECT test FROM " + tableName).getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("Hello", "hello测试􏿿world编码");
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + "(test varchar)");
        assertUpdate("INSERT INTO " + tableName + "(test) VALUES 'aa', 'bé'", 2);
        assertQuery("SELECT test FROM " + tableName, "VALUES 'aa', 'bé'");
        assertQuery("SELECT test FROM " + tableName + " WHERE test = 'aa'", "VALUES 'aa'");
        assertQuery("SELECT test FROM " + tableName + " WHERE test > 'ba'", "VALUES 'bé'");
        assertQuery("SELECT test FROM " + tableName + " WHERE test < 'ba'", "VALUES 'aa'");
        assertQueryReturnsEmptyResult("SELECT test FROM " + tableName + " WHERE test = 'ba'");
        assertUpdate("DROP TABLE " + tableName);

        assertUpdate("CREATE TABLE " + tableName + "(test varchar)");
        assertUpdate("INSERT INTO " + tableName + "(test) VALUES 'a', 'é'", 2);
        assertQuery("SELECT test FROM " + tableName, "VALUES 'a', 'é'");
        assertQuery("SELECT test FROM " + tableName + " WHERE test = 'a'", "VALUES 'a'");
        assertQuery("SELECT test FROM " + tableName + " WHERE test > 'b'", "VALUES 'é'");
        assertQuery("SELECT test FROM " + tableName + " WHERE test < 'b'", "VALUES 'a'");
        assertQueryReturnsEmptyResult("SELECT test FROM " + tableName + " WHERE test = 'b'");
        assertUpdate("DROP TABLE " + tableName);
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

        assertUpdate("CREATE TABLE " + tableName + " (a ARRAY<DOUBLE>, b ARRAY<BIGINT>)");

        assertUpdate("INSERT INTO " + tableName + " (a) VALUES (ARRAY[null])", 1);
        assertUpdate("INSERT INTO " + tableName + " (a, b) VALUES (ARRAY[1.23E1], ARRAY[1.23E1])", 1);
        assertQuery("SELECT a[1], b[1] FROM " + tableName, "VALUES (null, null), (12.3, 12)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDelete()
    {
        if (!supportsDelete()) {
            assertQueryFails("DELETE FROM nation__c", "This connector does not support deletes");
            return;
        }

        String tableName = "test_delete_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM orders__c", "SELECT count(*) FROM orders__c");

        // delete half the table, then delete the rest
        assertUpdate("DELETE FROM " + tableName + " WHERE orderkey__c % 2 = 0", "SELECT count(*) FROM orders__c WHERE orderkey__c % 2 = 0");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders__c WHERE orderkey__c % 2 <> 0");

        assertUpdate("DELETE FROM " + tableName, "SELECT count(*) FROM orders__c WHERE orderkey__c % 2 <> 0");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders__c LIMIT 0");

        assertUpdate("DROP TABLE " + tableName);

        // delete successive parts of the table

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM orders__c", "SELECT count(*) FROM orders__c");

        assertUpdate("DELETE FROM " + tableName + " WHERE custkey__c <= 100", "SELECT count(*) FROM orders__c WHERE custkey__c <= 100");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders__c WHERE custkey__c > 100");

        assertUpdate("DELETE FROM " + tableName + " WHERE custkey__c <= 300", "SELECT count(*) FROM orders__c WHERE custkey__c > 100 AND custkey__c <= 300");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders__c WHERE custkey__c > 300");

        assertUpdate("DELETE FROM " + tableName + " WHERE custkey__c <= 500", "SELECT count(*) FROM orders__c WHERE custkey__c > 300 AND custkey__c <= 500");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders__c WHERE custkey__c > 500");

        assertUpdate("DROP TABLE " + tableName);

        // delete using a constant property

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM orders__c", "SELECT count(*) FROM orders__c");

        assertUpdate("DELETE FROM " + tableName + " WHERE orderstatus__c = 'O'", "SELECT count(*) FROM orders__c WHERE orderstatus__c = 'O'");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM orders__c WHERE orderstatus__c <> 'O'");

        assertUpdate("DROP TABLE " + tableName);

        // delete without matching any rows

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM orders__c", "SELECT count(*) FROM orders__c");
        assertUpdate("DELETE FROM " + tableName + " WHERE rand() < 0", 0);
        assertUpdate("DELETE FROM " + tableName + " WHERE orderkey__c < 0", 0);
        assertUpdate("DROP TABLE " + tableName);

        // delete with a predicate that optimizes to false

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM orders__c", "SELECT count(*) FROM orders__c");
        assertUpdate("DELETE FROM " + tableName + " WHERE orderkey__c > 5 AND orderkey__c < 4", 0);
        assertUpdate("DROP TABLE " + tableName);

        // delete using a subquery

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM lineitem__c", "SELECT count(*) FROM lineitem__c");

        assertUpdate(
                "DELETE FROM " + tableName + " WHERE orderkey__c IN (SELECT orderkey__c FROM orders__c WHERE orderstatus__c = 'F')",
                "SELECT count(*) FROM lineitem__c WHERE orderkey__c IN (SELECT orderkey__c FROM orders__c WHERE orderstatus__c = 'F')");
        assertQuery(
                "SELECT * FROM " + tableName,
                "SELECT * FROM lineitem__c WHERE orderkey__c IN (SELECT orderkey__c FROM orders__c WHERE orderstatus__c <> 'F')");

        assertUpdate("DROP TABLE " + tableName);

        // delete with multiple SemiJoin

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM lineitem__c", "SELECT count(*) FROM lineitem__c");

        assertUpdate(
                "DELETE FROM " + tableName + "\n" +
                        "WHERE orderkey__c IN (SELECT orderkey__c FROM orders__c WHERE orderstatus__c = 'F')\n" +
                        "  AND orderkey__c IN (SELECT orderkey__c FROM orders__c WHERE custkey__c % 5 = 0)\n",
                "SELECT count(*) FROM lineitem__c\n" +
                        "WHERE orderkey__c IN (SELECT orderkey__c FROM orders__c WHERE orderstatus__c = 'F')\n" +
                        "  AND orderkey__c IN (SELECT orderkey__c FROM orders__c WHERE custkey__c % 5 = 0)");
        assertQuery(
                "SELECT * FROM " + tableName,
                "SELECT * FROM lineitem__c\n" +
                        "WHERE orderkey__c IN (SELECT orderkey__c FROM orders__c WHERE orderstatus__c <> 'F')\n" +
                        "  OR orderkey__c IN (SELECT orderkey__c FROM orders__c WHERE custkey__c % 5 <> 0)");

        assertUpdate("DROP TABLE " + tableName);

        // delete with SemiJoin null handling

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM orders__c", "SELECT count(*) FROM orders__c");

        assertUpdate(
                "DELETE FROM " + tableName + "\n" +
                        "WHERE (orderkey__c IN (SELECT CASE WHEN orderkey__c % 3 = 0 THEN NULL ELSE orderkey__c END FROM lineitem__c)) IS NULL\n",
                "SELECT count(*) FROM orders__c\n" +
                        "WHERE (orderkey__c IN (SELECT CASE WHEN orderkey__c % 3 = 0 THEN NULL ELSE orderkey__c END FROM lineitem__c)) IS NULL\n");
        assertQuery(
                "SELECT * FROM " + tableName,
                "SELECT * FROM orders__c\n" +
                        "WHERE (orderkey__c IN (SELECT CASE WHEN orderkey__c % 3 = 0 THEN NULL ELSE orderkey__c END FROM lineitem__c)) IS NOT NULL\n");

        assertUpdate("DROP TABLE " + tableName);

        // delete using a scalar and EXISTS subquery
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM orders__c", "SELECT count(*) FROM orders__c");
        assertUpdate("DELETE FROM " + tableName + " WHERE orderkey__c = (SELECT orderkey__c FROM orders__c ORDER BY orderkey__c LIMIT 1)", 1);
        assertUpdate("DELETE FROM " + tableName + " WHERE orderkey__c = (SELECT orderkey__c FROM orders__c WHERE false)", 0);
        assertUpdate("DELETE FROM " + tableName + " WHERE EXISTS(SELECT 1 WHERE false)", 0);
        assertUpdate("DELETE FROM " + tableName + " WHERE EXISTS(SELECT 1)", "SELECT count(*) - 1 FROM orders__c");
        assertUpdate("DROP TABLE " + tableName);

        // test EXPLAIN ANALYZE with CTAS
        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE " + tableName + " AS SELECT CAST(orderstatus__c AS VARCHAR(15)) orderstatus__c FROM orders__c");
        assertQuery("SELECT * from " + tableName, "SELECT orderstatus__c FROM orders__c");
        // check that INSERT works also
        assertExplainAnalyze("EXPLAIN ANALYZE INSERT INTO " + tableName + " SELECT clerk__c FROM orders__c");
        assertQuery("SELECT * from " + tableName, "SELECT orderstatus__c FROM orders__c UNION ALL SELECT clerk__c FROM orders__c");
        // check DELETE works with EXPLAIN ANALYZE
        assertExplainAnalyze("EXPLAIN ANALYZE DELETE FROM " + tableName + " WHERE TRUE");
        assertQuery("SELECT COUNT(*) from " + tableName, "SELECT 0");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropTableIfExists()
    {
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_if_exists"));
        assertUpdate("DROP TABLE IF EXISTS test_drop_if_exists");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_if_exists"));
    }

    @Test
    public void testView()
    {
        skipTestUnless(supportsViews());

        @Language("SQL") String query = "SELECT orderkey__c, orderstatus__c, totalprice__c / 2 half FROM orders__c";

        String testView = "test_view_" + randomTableSuffix();
        String testViewWithComment = "test_view_with_comment_" + randomTableSuffix();
        assertUpdate("CREATE VIEW " + testView + " AS SELECT 123 x");
        assertUpdate("CREATE OR REPLACE VIEW " + testView + " AS " + query);

        assertUpdate("CREATE VIEW " + testViewWithComment + " COMMENT 'orders__c' AS SELECT 123 x");
        assertUpdate("CREATE OR REPLACE VIEW " + testViewWithComment + " COMMENT 'orders__c' AS " + query);

        MaterializedResult materializedRows = computeActual("SHOW CREATE VIEW " + testViewWithComment);
        assertThat((String) materializedRows.getOnlyValue()).contains("COMMENT 'orders__c'");

        assertQuery("SELECT * FROM " + testView, query);
        assertQuery("SELECT * FROM " + testViewWithComment, query);

        assertQuery(
                "SELECT * FROM " + testView + " a JOIN " + testView + " b on a.orderkey__c = b.orderkey__c",
                format("SELECT * FROM (%s) a JOIN (%s) b ON a.orderkey__c = b.orderkey__c", query, query));

        assertQuery("WITH orders__c AS (SELECT * FROM orders__c LIMIT 0) SELECT * FROM " + testView, query);

        String name = format("%s.%s." + testView, getSession().getCatalog().get(), getSession().getSchema().get());
        assertQuery("SELECT * FROM " + name, query);

        assertUpdate("DROP VIEW " + testView);
        assertUpdate("DROP VIEW " + testViewWithComment);
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
                .row("customer__c", "BASE TABLE")
                .row("lineitem__c", "BASE TABLE")
                .row(viewName, "VIEW")
                .row("nation__c", "BASE TABLE")
                .row("orders__c", "BASE TABLE")
                .row("region__c", "BASE TABLE")
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

        org.testng.Assert.assertEquals(getOnlyElement(actual.getOnlyColumnAsSet()), expectedSql);

        actual = computeActual(format("SHOW CREATE VIEW %s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), viewName));

        org.testng.Assert.assertEquals(getOnlyElement(actual.getOnlyColumnAsSet()), expectedSql);

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
    public void testQueryLoggingCount()
    {
        skipTestUnless(supportsCreateTable());

        QueryManager queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();
        executeExclusively(() -> {
            assertEventually(
                    new Duration(1, MINUTES),
                    () -> org.testng.Assert.assertEquals(
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
            String tableName = "test_query_logging_count" + randomTableSuffix();
            assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 foo_1, 2 foo_2_4", 1);
            assertQuery("SELECT foo_1, foo_2_4 FROM " + tableName, "SELECT 1, 2");
            assertUpdate("DROP TABLE " + tableName);
            assertQueryFails("SELECT * FROM " + tableName, ".*Table .* does not exist");

            // TODO: Figure out a better way of synchronization
            assertEventually(
                    new Duration(1, MINUTES),
                    () -> org.testng.Assert.assertEquals(dispatchManager.getStats().getCompletedQueries().getTotalCount() - beforeCompletedQueriesCount, 4));
            org.testng.Assert.assertEquals(dispatchManager.getStats().getSubmittedQueries().getTotalCount() - beforeSubmittedQueriesCount, 4);
        });
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
        String sql = "CREATE TABLE " + tableName + " AS SELECT * FROM nation__c";
        ResultWithQueryId<MaterializedResult> resultResultWithQueryId = getDistributedQueryRunner().executeWithQueryId(getSession(), sql);
        QueryInfo queryInfo = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());

        org.testng.Assert.assertEquals(queryInfo.getQueryStats().getOutputPositions(), 1L);
        org.testng.Assert.assertEquals(queryInfo.getQueryStats().getWrittenPositions(), 25L);
        assertTrue(queryInfo.getQueryStats().getLogicalWrittenDataSize().toBytes() > 0L);

        sql = "INSERT INTO " + tableName + " SELECT * FROM nation__c LIMIT 10";
        resultResultWithQueryId = getDistributedQueryRunner().executeWithQueryId(getSession(), sql);
        queryInfo = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());

        org.testng.Assert.assertEquals(queryInfo.getQueryStats().getOutputPositions(), 1L);
        org.testng.Assert.assertEquals(queryInfo.getQueryStats().getWrittenPositions(), 10L);
        assertTrue(queryInfo.getQueryStats().getLogicalWrittenDataSize().toBytes() > 0L);

        assertUpdate("DROP TABLE " + tableName);
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
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);
        assertQueryFails("CREATE SCHEMA " + schemaName, format("line 1:1: Schema '.*\\.%s' already exists", schemaName));
        assertUpdate("DROP SCHEMA " + schemaName);
        assertQueryFails("DROP SCHEMA " + schemaName, format("line 1:1: Schema '.*\\.%s' does not exist", schemaName));
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

    @Test(dataProvider = "testColumnNameDataProvider")
    public void testColumnName(String columnName)
    {
        skipTestUnless(supportsCreateTable());

        if (!requiresDelimiting(columnName)) {
            testColumnName(columnName, false);
        }
        testColumnName(columnName, true);
    }

    private void testColumnName(String columnName, boolean delimited)
    {
        String nameInSql = columnName;
        if (delimited) {
            nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
        }
        String tableName = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "_") + "_" + randomTableSuffix();

        try {
            // TODO test with both CTAS *and* CREATE TABLE + INSERT, since they use different connector API methods.
            assertUpdate("CREATE TABLE " + tableName + "(key varchar, " + nameInSql + " varchar)");
        }
        catch (RuntimeException e) {
            if (isColumnNameRejected(e, columnName, delimited)) {
                // It is OK if give column name__c is not allowed and is clearly rejected by the connector.
                return;
            }
            throw e;
        }
        assertUpdate("INSERT INTO " + tableName + " VALUES ('null value', NULL), ('sample value', 'abc'), ('other value', 'xyz')", 3);

        // SELECT *
        assertQuery("SELECT * FROM " + tableName, "VALUES ('null value', NULL), ('sample value', 'abc'), ('other value', 'xyz')");

        // projection
        assertQuery("SELECT " + nameInSql + " FROM " + tableName, "VALUES (NULL), ('abc'), ('xyz')");

        // predicate
        assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " IS NULL", "VALUES ('null value')");
        assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " = 'abc'", "VALUES ('sample value')");

        assertUpdate("DROP TABLE " + tableName);
    }

    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return false;
    }

    private static boolean requiresDelimiting(String identifierName)
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
                    "SELECT CAST(row_id AS varchar) row_id, CAST(value AS " + trinoTypeName + ") value " +
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

    // BaseConnectorTest

    /**
     * @deprecated Use {@link #hasBehavior(TestingConnectorBehavior)} instead.
     */
    @Deprecated
    protected final boolean supportsCreateSchema()
    {
        return hasBehavior(SUPPORTS_CREATE_SCHEMA);
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
        assertQuery("SELECT shippriority__c, clerk__c, totalprice__c FROM orders__c");
    }

    @Test
    public void testAggregation()
    {
        assertQuery("SELECT sum(orderkey__c) FROM orders__c", "SELECT sum(orderkey) FROM orders");
        assertQuery("SELECT sum(totalprice__c) FROM orders__c", "SELECT sum(totalprice) FROM orders");
        assertQuery("SELECT max(comment__c) FROM nation__c");

        assertQuery("SELECT count(*) FROM orders__c");
        assertQuery("SELECT count(*) FROM orders__c WHERE orderkey__c > 10");
        assertQuery("SELECT count(*) FROM (SELECT * FROM orders__c LIMIT 10)");
        assertQuery("SELECT count(*) FROM (SELECT * FROM orders__c WHERE orderkey__c > 10 LIMIT 10)");

        assertQuery("SELECT DISTINCT regionkey__c FROM nation__c");
        assertQuery("SELECT regionkey__c FROM nation__c GROUP BY regionkey__c");

        // TODO support aggregation pushdown with GROUPING SETS
        assertQuery(
                "SELECT regionkey__c, nationkey__c FROM nation__c GROUP BY GROUPING SETS ((regionkey__c), (nationkey__c))",
                "SELECT NULL, nationkey FROM nation " +
                        "UNION ALL SELECT DISTINCT regionkey, NULL FROM nation");
        assertQuery(
                "SELECT regionkey__c, nationkey__c, count(*) FROM nation__c GROUP BY GROUPING SETS ((), (regionkey__c), (nationkey__c), (regionkey__c, nationkey__c))",
                "SELECT NULL, NULL, count(*) FROM nation " +
                        "UNION ALL SELECT NULL, nationkey, 1 FROM nation " +
                        "UNION ALL SELECT regionkey, NULL, count(*) FROM nation GROUP BY regionkey " +
                        "UNION ALL SELECT regionkey, nationkey, 1 FROM nation");

        assertQuery("SELECT count(regionkey__c) FROM nation__c");
        assertQuery("SELECT count(DISTINCT regionkey__c) FROM nation__c");
        assertQuery("SELECT regionkey__c, count(*) FROM nation__c GROUP BY regionkey__c");

        assertQuery("SELECT min(regionkey__c), max(regionkey__c) FROM nation__c");
        assertQuery("SELECT min(DISTINCT regionkey__c), max(DISTINCT regionkey__c) FROM nation__c");
        assertQuery("SELECT regionkey__c, min(regionkey__c), min(name__c), max(regionkey__c), max(name__c) FROM nation__c GROUP BY regionkey__c");

        assertQuery("SELECT sum(regionkey__c) FROM nation__c");
        assertQuery("SELECT sum(DISTINCT regionkey__c) FROM nation__c");
        assertQuery("SELECT regionkey__c, sum(regionkey__c) FROM nation__c GROUP BY regionkey__c");

        assertQuery(
                "SELECT avg(nationkey__c) FROM nation__c",
                "SELECT avg(CAST(nationkey AS double)) FROM nation");
        assertQuery(
                "SELECT avg(DISTINCT nationkey__c) FROM nation__c",
                "SELECT avg(DISTINCT CAST(nationkey AS double)) FROM nation");
        assertQuery(
                "SELECT regionkey__c, avg(nationkey__c) FROM nation__c GROUP BY regionkey__c",
                "SELECT regionkey, avg(CAST(nationkey AS double)) FROM nation GROUP BY regionkey");
    }

    @Test
    public void testExactPredicate()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM orders__c WHERE orderkey__c = 10");

        // filtered column is selected
        assertQuery("SELECT custkey__c, orderkey__c FROM orders__c WHERE orderkey__c = 32", "VALUES (1301, 32)");

        // filtered column is not selected
        assertQuery("SELECT custkey__c FROM orders__c WHERE orderkey__c = 32", "VALUES (1301)");
    }

    @Test
    public void testInListPredicate()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM orders__c WHERE orderkey__c IN (10, 11, 20, 21)");

        // filtered column is selected
        assertQuery("SELECT custkey__c, orderkey__c FROM orders__c WHERE orderkey__c IN (7, 10, 32, 33)", "VALUES (392, 7), (1301, 32), (670, 33)");

        // filtered column is not selected
        assertQuery("SELECT custkey__c FROM orders__c WHERE orderkey__c IN (7, 10, 32, 33)", "VALUES (392), (1301), (670)");
    }

    @Test
    public void testIsNullPredicate()
    {
        assertQueryReturnsEmptyResult("SELECT * FROM orders__c WHERE orderkey__c IS NULL");
        assertQueryReturnsEmptyResult("SELECT * FROM orders__c WHERE orderkey__c = 10 OR orderkey__c IS NULL");

        // filtered column is selected
        assertQuery("SELECT custkey__c, orderkey__c FROM orders__c WHERE orderkey__c = 32 OR orderkey__c IS NULL", "VALUES (1301, 32)");

        // filtered column is not selected
        assertQuery("SELECT custkey__c FROM orders__c WHERE orderkey__c = 32 OR orderkey__c IS NULL", "VALUES (1301)");
    }

    @Test
    public void testLikePredicate()
    {
        // filtered column is not selected
        assertQuery("SELECT orderkey__c FROM orders__c WHERE orderpriority__c LIKE '5-L%'");

        // filtered column is selected
        assertQuery("SELECT orderkey__c, orderpriority__c FROM orders__c WHERE orderpriority__c LIKE '5-L%'");

        // filtered column is not selected
        assertQuery("SELECT orderkey__c FROM orders__c WHERE orderpriority__c LIKE '5-L__'");

        // filtered column is selected
        assertQuery("SELECT orderkey__c, orderpriority__c FROM orders__c WHERE orderpriority__c LIKE '5-L__'");
    }

    @Test
    public void testMultipleRangesPredicate()
    {
        // Salesforce is stripping leading whitespace from the comments
        assertQuery("" +
                "SELECT orderkey__c, custkey__c, orderstatus__c, totalprice__c, orderdate__c, orderpriority__c, clerk__c, shippriority__c, TRIM(comment__c) " +
                "FROM orders__c " +
                "WHERE orderkey__c BETWEEN 10 AND 50");
    }

    @Test
    public void testRangePredicate()
    {
        // Salesforce is stripping leading whitespace from the comments
        assertQuery("" +
                "SELECT orderkey__c, custkey__c, orderstatus__c, totalprice__c, orderdate__c, orderpriority__c, clerk__c, shippriority__c, TRIM(comment__c) " +
                "FROM orders__c " +
                "WHERE orderkey__c BETWEEN 10 AND 50");
    }

    @Test
    public void testPredicateReflectedInExplain()
    {
        // Even if the predicate is pushed down into the table scan, it should still be reflected in EXPLAIN (via ConnectorTableHandle.toString)
        assertExplain(
                "EXPLAIN SELECT name__c FROM nation__c WHERE nationkey__c = 42",
                "(predicate|filterPredicate|constraint).{0,10}(nationkey__c|NATIONKEY)");
    }

    @Test
    public void testConcurrentScans()
    {
        // Salesforce has a hard limit of 10 concurrent queries, otherwise you'll get a INVALID_QUERY_LOCATOR error
        // We do five copies to leave room for the other thread that is running test
        // See https://help.salesforce.com/articleView?id=000323582&type=1&mode=1
        String unionMultipleTimes = join(" UNION ALL ", nCopies(5, "SELECT * FROM orders__c"));
        assertQuery("SELECT sum(if(rand() >= 0, orderkey__c)) FROM (" + unionMultipleTimes + ")", "VALUES 2249362500");
    }

    @Test
    public void testSelectAll()
    {
        // TODO Not sure what is going on here, getting an error from H2, thinks this date is a double type
        //   Caused by: org.h2.jdbc.JdbcSQLDataException: Data conversion error converting "1996-01-02" [22018-200]
        throw new SkipException("testSelectAll");
    }

    /**
     * Test interactions between optimizer (including CBO), scheduling and connector metadata APIs.
     */
    @Test(timeOut = 300_000, dataProvider = "joinDistributionTypes")
    public void testJoinWithEmptySides(FeaturesConfig.JoinDistributionType joinDistributionType)
    {
        Session session = noJoinReordering(joinDistributionType);
        // empty build side
        assertQuery(session, "SELECT count(*) FROM nation__c JOIN region__c ON nation__c.regionkey__c = region__c.regionkey__c AND region__c.name__c = ''", "VALUES 0");
        assertQuery(session, "SELECT count(*) FROM nation__c JOIN region__c ON nation__c.regionkey__c = region__c.regionkey__c AND region__c.regionkey__c < 0", "VALUES 0");
        // empty probe side
        assertQuery(session, "SELECT count(*) FROM region__c JOIN nation__c ON nation__c.regionkey__c = region__c.regionkey__c AND region__c.name__c = ''", "VALUES 0");
        assertQuery(session, "SELECT count(*) FROM nation__c JOIN region__c ON nation__c.regionkey__c = region__c.regionkey__c AND region__c.regionkey__c < 0", "VALUES 0");
    }

    @DataProvider
    public Object[][] joinDistributionTypes()
    {
        return Stream.of(FeaturesConfig.JoinDistributionType.values())
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
                "SELECT c.name__c, n.name__c, r.name__c " +
                        "FROM nation__c n " +
                        "JOIN customer__c c ON c.nationkey__c = n.nationkey__c " +
                        "JOIN region__c r ON n.regionkey__c = r.regionkey__c");

        // 2 inner joins, eligible for join reodering, where one table has a filter
        assertQuery(
                session,
                "SELECT c.name__c, n.name__c, r.name__c " +
                        "FROM nation__c n " +
                        "JOIN customer__c c ON c.nationkey__c = n.nationkey__c " +
                        "JOIN region__c r ON n.regionkey__c = r.regionkey__c " +
                        "WHERE n.name__c = 'ARGENTINA'");

        // 2 inner joins, eligible for join reodering, on top of aggregation
        assertQuery(
                session,
                "SELECT c.name__c, n.name__c, n.count, r.name__c " +
                        "FROM (SELECT name__c, regionkey__c, nationkey__c, count(*) count FROM nation__c GROUP BY name__c, regionkey__c, nationkey__c) n " +
                        "JOIN customer__c c ON c.nationkey__c = n.nationkey__c " +
                        "JOIN region__c r ON n.regionkey__c = r.regionkey__c");
    }

    @Test
    public void testDescribeTable()
    {
        MaterializedResult expectedColumns = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("id", "varchar(18)", "", "Label Record ID corresponds to this field.")
                .row("ownerid", "varchar(18)", "", "Label Owner ID corresponds to this field.")
                .row("isdeleted", "boolean", "", "Label Deleted corresponds to this field.")
                .row("name", "varchar(80)", "", "Label Name corresponds to this field.")
                .row("createddate", "timestamp(0)", "", "Label Created Date corresponds to this field.")
                .row("createdbyid", "varchar(18)", "", "Label Created By ID corresponds to this field.")
                .row("lastmodifieddate", "timestamp(0)", "", "Label Last Modified Date corresponds to this field.")
                .row("lastmodifiedbyid", "varchar(18)", "", "Label Last Modified By ID corresponds to this field.")
                .row("systemmodstamp", "timestamp(0)", "", "Label System Modstamp corresponds to this field.")
                .row("lastactivitydate", "date", "", "Label Last Activity Date corresponds to this field.")
                .row("shippriority__c", "double", "", "Label shippriority corresponds to this field.")
                .row("custkey__c", "double", "", "Label custkey corresponds to this field.")
                .row("orderstatus__c", "varchar(1)", "", "Label orderstatus corresponds to this field.")
                .row("totalprice__c", "double", "", "Label totalprice corresponds to this field.")
                .row("orderkey__c", "double", "", "Label orderkey corresponds to this field.")
                .row("comment__c", "varchar(79)", "", "Label comment corresponds to this field.")
                .row("orderdate__c", "date", "", "Label orderdate corresponds to this field.")
                .row("orderpriority__c", "varchar(15)", "", "Label orderpriority corresponds to this field.")
                .row("clerk__c", "varchar(15)", "", "Label clerk corresponds to this field.")
                .build();
        MaterializedResult actualColumns = computeActual("DESCRIBE orders__c");
        assertEquals(actualColumns, expectedColumns);
    }

    @Test
    public void testExplainAnalyze()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT * FROM orders__c");
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk__c FROM orders__c GROUP BY clerk__c");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT x + y FROM (" +
                        "   SELECT orderdate__c, COUNT(*) x FROM orders__c GROUP BY orderdate__c) a JOIN (" +
                        "   SELECT orderdate__c, COUNT(*) y FROM orders__c GROUP BY orderdate__c) b ON a.orderdate__c = b.orderdate__c");
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk__c FROM orders__c GROUP BY clerk__c UNION ALL SELECT sum(orderkey__c), clerk__c FROM orders__c GROUP BY clerk__c");

        assertExplainAnalyze("EXPLAIN ANALYZE SHOW COLUMNS FROM orders__c");
        assertExplainAnalyze("EXPLAIN ANALYZE EXPLAIN SELECT count(*) FROM orders__c");
        assertExplainAnalyze("EXPLAIN ANALYZE EXPLAIN ANALYZE SELECT count(*) FROM orders__c");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW FUNCTIONS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW TABLES");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW SCHEMAS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW CATALOGS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW SESSION");
    }

    @Test
    public void testExplainAnalyzeVerbose()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT * FROM orders__c");
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT rank() OVER (PARTITION BY orderkey__c ORDER BY clerk__c DESC) FROM orders__c");
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT rank() OVER (PARTITION BY orderkey__c ORDER BY clerk__c DESC) FROM orders__c WHERE orderkey__c < 0");
    }

    @Test
    public void testTableSampleSystem()
    {
        MaterializedResult fullSample = computeActual("SELECT orderkey__c FROM orders__c TABLESAMPLE SYSTEM (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey__c FROM orders__c TABLESAMPLE SYSTEM (0)");
        MaterializedResult randomSample = computeActual("SELECT orderkey__c FROM orders__c TABLESAMPLE SYSTEM (50)");
        MaterializedResult all = computeActual("SELECT orderkey__c FROM orders__c");

        assertContains(all, fullSample);
        assertEquals(emptySample.getMaterializedRows().size(), 0);
        assertTrue(all.getMaterializedRows().size() >= randomSample.getMaterializedRows().size());
    }

    @Test
    public void testTableSampleWithFiltering()
    {
        MaterializedResult emptySample = computeActual("SELECT DISTINCT orderkey__c, orderdate__c FROM orders__c TABLESAMPLE SYSTEM (99) WHERE orderkey__c BETWEEN 0 AND 0");
        MaterializedResult halfSample = computeActual("SELECT DISTINCT orderkey__c, orderdate__c FROM orders__c TABLESAMPLE SYSTEM (50) WHERE orderkey__c BETWEEN 0 AND 9999999999");
        MaterializedResult all = computeActual("SELECT orderkey__c, orderdate__c FROM orders__c");

        assertEquals(emptySample.getMaterializedRows().size(), 0);
        // Assertions need to be loose here because SYSTEM sampling random selects data on split boundaries. In this case either all the data will be selected, or
        // none of it. Sampling with a 100% ratio is ignored, so that also cannot be used to guarantee results.
        assertTrue(all.getMaterializedRows().size() >= halfSample.getMaterializedRows().size());
    }

    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders__c").getOnlyValue())
                // If the connector reports additional column properties, the expected value needs to be adjusted in the test subclass
                .matches("CREATE TABLE \\w+\\.\\w+\\.orders__c \\Q(\n" +
                        "   id varchar(18) NOT NULL COMMENT 'Label Record ID corresponds to this field.',\n" +
                        "   ownerid varchar(18) NOT NULL COMMENT 'Label Owner ID corresponds to this field.',\n" +
                        "   isdeleted boolean NOT NULL COMMENT 'Label Deleted corresponds to this field.',\n" +
                        "   name varchar(80) COMMENT 'Label Name corresponds to this field.',\n" +
                        "   createddate timestamp(0) NOT NULL COMMENT 'Label Created Date corresponds to this field.',\n" +
                        "   createdbyid varchar(18) NOT NULL COMMENT 'Label Created By ID corresponds to this field.',\n" +
                        "   lastmodifieddate timestamp(0) COMMENT 'Label Last Modified Date corresponds to this field.',\n" +
                        "   lastmodifiedbyid varchar(18) COMMENT 'Label Last Modified By ID corresponds to this field.',\n" +
                        "   systemmodstamp timestamp(0) NOT NULL COMMENT 'Label System Modstamp corresponds to this field.',\n" +
                        "   lastactivitydate date COMMENT 'Label Last Activity Date corresponds to this field.',\n" +
                        "   shippriority__c double COMMENT 'Label shippriority corresponds to this field.',\n" +
                        "   custkey__c double COMMENT 'Label custkey corresponds to this field.',\n" +
                        "   orderstatus__c varchar(1) COMMENT 'Label orderstatus corresponds to this field.',\n" +
                        "   totalprice__c double COMMENT 'Label totalprice corresponds to this field.',\n" +
                        "   orderkey__c double COMMENT 'Label orderkey corresponds to this field.',\n" +
                        "   comment__c varchar(79) COMMENT 'Label comment corresponds to this field.',\n" +
                        "   orderdate__c date COMMENT 'Label orderdate corresponds to this field.',\n" +
                        "   orderpriority__c varchar(15) COMMENT 'Label orderpriority corresponds to this field.',\n" +
                        "   clerk__c varchar(15) COMMENT 'Label clerk corresponds to this field.'\n" +
                        ")");
    }

    @Test
    public void testSelectInformationSchemaTables()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String schemaPattern = schema.replaceAll("^.", "_");

        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema + "' AND table_name = 'orders__c'", "VALUES 'orders__c'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema LIKE '" + schema + "' AND table_name LIKE '%rders__c'", "VALUES 'orders__c'");
        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '%rders__c'", "VALUES 'orders__c'");
        assertQuery(
                "SELECT table_name FROM information_schema.tables " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema LIKE '" + schema + "' AND table_name LIKE '%orders__c'",
                "VALUES 'orders__c'");
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
        checkState(getSession().getCatalog().isPresent());
        checkState(getSession().getSchema().isPresent());

        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String schemaPattern = schema.replaceAll(".$", "_");

        @Language("SQL") String ordersTableWithColumns = "VALUES " +
                "('orders__c', 'id'), " +
                "('orders__c', 'ownerid'), " +
                "('orders__c', 'isdeleted'), " +
                "('orders__c', 'name'), " +
                "('orders__c', 'createddate'), " +
                "('orders__c', 'createdbyid'), " +
                "('orders__c', 'lastmodifieddate'), " +
                "('orders__c', 'lastmodifiedbyid'), " +
                "('orders__c', 'systemmodstamp'), " +
                "('orders__c', 'lastactivitydate'), " +
                "('orders__c', 'orderkey__c'), " +
                "('orders__c', 'custkey__c'), " +
                "('orders__c', 'orderstatus__c'), " +
                "('orders__c', 'totalprice__c'), " +
                "('orders__c', 'orderdate__c'), " +
                "('orders__c', 'orderpriority__c'), " +
                "('orders__c', 'clerk__c'), " +
                "('orders__c', 'shippriority__c'), " +
                "('orders__c', 'comment__c')";

        assertQuery("SELECT table_schema FROM information_schema.columns WHERE table_schema = '" + schema + "' GROUP BY table_schema", "VALUES '" + schema + "'");
        assertQuery("SELECT table_name FROM information_schema.columns WHERE table_name = 'orders__c' GROUP BY table_name", "VALUES 'orders__c'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'orders__c'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%rders__c'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '_rders___'", ordersTableWithColumns);
        assertQuery(
                "SELECT table_name, column_name FROM information_schema.columns " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '%orders__c%'",
                ordersTableWithColumns);

        assertQuerySucceeds("SELECT * FROM information_schema.columns");
        assertQuery("SELECT DISTINCT table_name, column_name FROM information_schema.columns WHERE table_name LIKE '_rders__c'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "'");
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '_rders__c'", ordersTableWithColumns);
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

    // BaseJdbcConnectorTest

    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_AGGREGATION_PUSHDOWN:
            case SUPPORTS_ARRAY:
            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_RENAME_SCHEMA:
            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_CREATE_TABLE_WITH_DATA:
            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_DELETE:
            case SUPPORTS_INSERT:
            case SUPPORTS_JOIN_PUSHDOWN:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN:
            case SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM:
            case SUPPORTS_LIMIT_PUSHDOWN:
            case SUPPORTS_ROW_LEVEL_DELETE:
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;
            case SUPPORTS_PREDICATE_PUSHDOWN:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY:
            case SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY:
            default:
                return true;
        }
    }

    @Test
    public void testTopNPushdownDisabled()
    {
        Session topNPushdownDisabled = Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().orElseThrow(), "topn_pushdown_enabled", "false")
                .build();
        assertThat(query(topNPushdownDisabled, "SELECT orderkey__c FROM orders__c ORDER BY orderkey__c LIMIT 10"))
                .ordered()
                .isNotFullyPushedDown(TopNNode.class);
    }

    @Test
    public void testTopNPushdown()
    {
        if (!hasBehavior(SUPPORTS_TOPN_PUSHDOWN)) {
            assertThat(query("SELECT orderkey__c FROM orders__c ORDER BY orderkey__c LIMIT 10"))
                    .ordered()
                    .isNotFullyPushedDown(TopNNode.class);
            return;
        }

        assertThat(query("SELECT orderkey__c FROM orders__c ORDER BY orderkey__c LIMIT 10"))
                .ordered()
                .isFullyPushedDown();

        assertThat(query("SELECT orderkey__c FROM orders__c ORDER BY orderkey__c DESC LIMIT 10"))
                .ordered()
                .isFullyPushedDown();

        // multiple sort columns with different orders__c
        assertThat(query("SELECT * FROM orders__c ORDER BY shippriority__c DESC, totalprice__c ASC LIMIT 10"))
                .ordered()
                .isFullyPushedDown();

        // TopN over aggregation column
        assertThat(query("SELECT sum(totalprice__c) AS total FROM orders__c GROUP BY custkey__c ORDER BY total DESC LIMIT 10"))
                .ordered()
                .isFullyPushedDown();

        // TopN over TopN
        assertThat(query("SELECT orderkey__c, totalprice__c FROM (SELECT orderkey__c, totalprice__c FROM orders__c ORDER BY 1, 2 LIMIT 10) ORDER BY 2, 1 LIMIT 5"))
                .ordered()
                .isFullyPushedDown();

        assertThat(query("" +
                "SELECT orderkey__c, totalprice__c " +
                "FROM (SELECT orderkey__c, totalprice__c FROM (SELECT orderkey__c, totalprice__c FROM orders__c ORDER BY 1, 2 LIMIT 10) " +
                "ORDER BY 2, 1 LIMIT 5) ORDER BY 1, 2 LIMIT 3"))
                .ordered()
                .isFullyPushedDown();

        // TopN over limit
        assertThat(query("SELECT orderkey__c, totalprice__c FROM (SELECT orderkey__c, totalprice__c FROM orders__c LIMIT 20) ORDER BY totalprice__c ASC LIMIT 5"))
                .ordered()
                .isFullyPushedDown();

        // TopN over limit with filter
        assertThat(query("" +
                "SELECT orderkey__c, totalprice__c " +
                "FROM (SELECT orderkey__c, totalprice__c FROM orders__c WHERE orderpriority__c = '1-URGENT' LIMIT 20) " +
                "ORDER BY totalprice__c ASC LIMIT 5"))
                .ordered()
                .isFullyPushedDown();

        // TopN over aggregation with filter
        assertThat(query("" +
                "SELECT * " +
                "FROM (SELECT SUM(totalprice__c) as sum, custkey__c AS total FROM orders__c GROUP BY custkey__c HAVING COUNT(*) > 3) " +
                "ORDER BY sum DESC LIMIT 10"))
                .ordered()
                .isFullyPushedDown();
    }

    @Test
    public void testCaseSensitiveTopNPushdown()
    {
        if (!hasBehavior(SUPPORTS_TOPN_PUSHDOWN)) {
            // Covered by testTopNPushdown
            return;
        }

        PlanMatchPattern topNOverTableScan = node(TopNNode.class, anyTree(node(TableScanNode.class)));

        try (TestTable testTable = new TestTable(
                getQueryRunner()::execute,
                "test_case_sensitive_topn_pushdown",
                "(a_string varchar(10), a_char char(10), a_bigint bigint)",
                List.of(
                        "'A', 'A', 1",
                        "'B', 'B', 2",
                        "'a', 'a', 3",
                        "'b', 'b', 4"))) {
            assertThat(query(getSession(), "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_string ASC LIMIT 2")).isNotFullyPushedDown(topNOverTableScan);
            assertThat(query(getSession(), "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_string DESC LIMIT 2")).isNotFullyPushedDown(topNOverTableScan);
            assertThat(query(getSession(), "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_char ASC LIMIT 2")).isNotFullyPushedDown(topNOverTableScan);
            assertThat(query(getSession(), "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_char DESC LIMIT 2")).isNotFullyPushedDown(topNOverTableScan);

            // multiple sort columns with at-least one case-sensitive column
            assertThat(query(getSession(), "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_bigint, a_char LIMIT 2")).isNotFullyPushedDown(topNOverTableScan);
            assertThat(query(getSession(), "SELECT a_bigint FROM " + testTable.getName() + " ORDER BY a_bigint, a_string DESC LIMIT 2")).isNotFullyPushedDown(topNOverTableScan);
        }
    }

    @Test
    public void testJoinPushdownDisabled()
    {
        // If join pushdown gets enabled by default, this test should use a session with join pushdown disabled
        Session noJoinPushdown = Session.builder(getSession())
                // Disable dynamic filtering so that expected plans in case of no pushdown remain "simple"
                .setSystemProperty("enable_dynamic_filtering", "false")
                // Disable optimized hash generation so that expected plans in case of no pushdown remain "simple"
                .setSystemProperty("optimize_hash_generation", "false")
                .build();

        PlanMatchPattern partitionedJoinOverTableScans = node(JoinNode.class,
                exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.REPARTITION,
                        node(TableScanNode.class)),
                exchange(ExchangeNode.Scope.LOCAL, ExchangeNode.Type.REPARTITION,
                        exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.REPARTITION,
                                node(TableScanNode.class))));

        assertThat(query(noJoinPushdown, "SELECT r.name__c, n.name__c FROM nation__c n JOIN region__c r ON n.regionkey__c = r.regionkey__c"))
                .isNotFullyPushedDown(partitionedJoinOverTableScans);
    }

    /**
     * Verify !SUPPORTS_JOIN_PUSHDOWN declaration is true.
     */
    @Test
    public void verifySupportsJoinPushdownDeclaration()
    {
        if (hasBehavior(SUPPORTS_JOIN_PUSHDOWN)) {
            // Covered by testJoinPushdown
            return;
        }

        assertThat(query(joinPushdownEnabled(getSession()), "SELECT r.name__c, n.name__c FROM nation__c n JOIN region__c r ON n.regionkey__c = r.regionkey__c"))
                .isNotFullyPushedDown(
                        node(JoinNode.class,
                                anyTree(node(TableScanNode.class)),
                                anyTree(node(TableScanNode.class))));
    }

    @Test
    public void testJoinPushdown()
    {
        PlanMatchPattern joinOverTableScans =
                node(JoinNode.class,
                        anyTree(node(TableScanNode.class)),
                        anyTree(node(TableScanNode.class)));

        PlanMatchPattern broadcastJoinOverTableScans =
                node(JoinNode.class,
                        node(TableScanNode.class),
                        exchange(ExchangeNode.Scope.LOCAL, ExchangeNode.Type.GATHER,
                                exchange(ExchangeNode.Scope.REMOTE, ExchangeNode.Type.REPLICATE,
                                        node(TableScanNode.class))));

        if (!hasBehavior(SUPPORTS_JOIN_PUSHDOWN)) {
            assertThat(query("SELECT r.name__c, n.name__c FROM nation__c n JOIN region__c r ON n.regionkey__c = r.regionkey__c"))
                    .isNotFullyPushedDown(joinOverTableScans);
            return;
        }

        Session session = joinPushdownEnabled(getSession());

        // Disable DF here for the sake of negative test cases' expected plan. With DF enabled, some operators return in DF's FilterNode and some do not.
        Session withoutDynamicFiltering = Session.builder(session)
                .setSystemProperty("enable_dynamic_filtering", "false")
                .build();

        String notDistinctOperator = "IS NOT DISTINCT FROM";
        List<String> nonEqualities = Stream.concat(
                Stream.of(JoinCondition.Operator.values())
                        .filter(operator -> operator != JoinCondition.Operator.EQUAL)
                        .map(JoinCondition.Operator::getValue),
                Stream.of(notDistinctOperator))
                .collect(toImmutableList());

        try (TestTable nationLowercaseTable = new TestTable(
                // If a connector supports Join pushdown, but does not allow CTAS, we need to make the table creation here overridable.
                getQueryRunner()::execute,
                "nation_lowercase",
                "AS SELECT nationkey__c, lower(name__c) name__c, regionkey__c FROM nation__c")) {
            // basic case
            assertThat(query(session, "SELECT r.name__c, n.name__c FROM nation__c n JOIN region__c r ON n.regionkey__c = r.regionkey__c")).isFullyPushedDown();

            // join over different columns
            assertThat(query(session, "SELECT r.name__c, n.name__c FROM nation__c n JOIN region__c r ON n.nationkey__c = r.regionkey__c")).isFullyPushedDown();

            // pushdown when using USING
            assertThat(query(session, "SELECT r.name__c, n.name__c FROM nation__c n JOIN region__c r USING(regionkey__c)")).isFullyPushedDown();

            // varchar equality predicate
            assertThat(query(session, "SELECT n.name__c, n2.regionkey__c FROM nation__c n JOIN nation__c n2 ON n.name__c = n2.name__c")).isFullyPushedDown();
            assertThat(query(session, format("SELECT n.name__c, nl.regionkey__c FROM nation__c n JOIN %s nl ON n.name__c = nl.name__c", nationLowercaseTable.getName())))
                    .isFullyPushedDown();

            // multiple bigint predicates
            assertThat(query(session, "SELECT n.name__c, c.name__c FROM nation__c n JOIN customer__c c ON n.nationkey__c = c.nationkey__c and n.regionkey__c = c.custkey__c"))
                    .isFullyPushedDown();

            // inequality
            for (String operator : nonEqualities) {
                // bigint inequality predicate
                assertThat(query(withoutDynamicFiltering, format("SELECT r.name__c, n.name__c FROM nation__c n JOIN region__c r ON n.regionkey__c %s r.regionkey__c", operator)))
                        // Currently no pushdown as inequality predicate is removed from Join to maintain Cross Join and Filter as separate nodes
                        .isNotFullyPushedDown(broadcastJoinOverTableScans);

                // varchar inequality predicate
                assertThat(query(withoutDynamicFiltering, format("SELECT n.name__c, nl.name__c FROM nation__c n JOIN %s nl ON n.name__c %s nl.name__c", nationLowercaseTable.getName(), operator)))
                        // Currently no pushdown as inequality predicate is removed from Join to maintain Cross Join and Filter as separate nodes
                        .isNotFullyPushedDown(broadcastJoinOverTableScans);
            }

            // inequality along with an equality, which constitutes an equi-condition and allows filter to remain as part of the Join
            for (String operator : nonEqualities) {
                assertConditionallyPushedDown(
                        session,
                        format("SELECT n.name__c, c.name__c FROM nation__c n JOIN customer__c c ON n.nationkey__c = c.nationkey__c AND n.regionkey__c %s c.custkey__c", operator),
                        expectJoinPushdown(operator),
                        joinOverTableScans);
            }

            // varchar inequality along with an equality, which constitutes an equi-condition and allows filter to remain as part of the Join
            for (String operator : nonEqualities) {
                assertConditionallyPushedDown(
                        session,
                        format("SELECT n.name__c, nl.name__c FROM nation__c n JOIN %s nl ON n.regionkey__c = nl.regionkey__c AND n.name__c %s nl.name__c", nationLowercaseTable.getName(), operator),
                        expectVarcharJoinPushdown(operator),
                        joinOverTableScans);
            }

            // LEFT JOIN
            assertThat(query(session, "SELECT r.name__c, n.name__c FROM nation__c n LEFT JOIN region__c r ON n.nationkey__c = r.regionkey__c")).isFullyPushedDown();
            assertThat(query(session, "SELECT r.name__c, n.name__c FROM region__c r LEFT JOIN nation__c n ON n.nationkey__c = r.regionkey__c")).isFullyPushedDown();

            // RIGHT JOIN
            assertThat(query(session, "SELECT r.name__c, n.name__c FROM nation__c n RIGHT JOIN region__c r ON n.nationkey__c = r.regionkey__c")).isFullyPushedDown();
            assertThat(query(session, "SELECT r.name__c, n.name__c FROM region__c r RIGHT JOIN nation__c n ON n.nationkey__c = r.regionkey__c")).isFullyPushedDown();

            // FULL JOIN
            assertThat(query(session, "SELECT r.name__c, n.name__c FROM nation__c n FULL JOIN region__c r ON n.nationkey__c = r.regionkey__c"))
                    .isNotFullyPushedDown(joinOverTableScans);

            // Join over a (double) predicate
            assertThat(query(session, "" +
                    "SELECT c.name__c, n.name__c " +
                    "FROM (SELECT * FROM customer__c WHERE acctbal > 8000) c " +
                    "JOIN nation__c n ON c.custkey__c = n.nationkey__c"))
                    .isFullyPushedDown();

            // Join over a varchar equality predicate
            assertThat(query(session,
                    "SELECT c.name__c, n.name__c FROM (SELECT * FROM customer__c WHERE address = 'TcGe5gaZNgVePxU5kRrvXBfkasDTea') c " +
                            "JOIN nation__c n ON c.custkey__c = n.nationkey__c"))
                    .isFullyPushedDown();

            // Join over a varchar inequality predicate
            assertThat(query(session,
                    "SELECT c.name__c, n.name__c FROM (SELECT * FROM customer__c WHERE address < 'TcGe5gaZNgVePxU5kRrvXBfkasDTea') c " +
                            "JOIN nation__c n ON c.custkey__c = n.nationkey__c"))
                    .isFullyPushedDown();

            // join over aggregation
            assertThat(query(session,
                    "SELECT * FROM (SELECT regionkey__c rk, count(nationkey__c) c FROM nation__c GROUP BY regionkey__c) n " +
                            "JOIN region__c r ON n.rk = r.regionkey__c"))
                    .isNotFullyPushedDown(joinOverTableScans);

            // join over LIMIT
            assertThat(query(session,
                    "SELECT * FROM (SELECT nationkey__c FROM nation__c LIMIT 30) n " +
                            "JOIN region__c r ON n.nationkey__c = r.regionkey__c"))
                    .isNotFullyPushedDown(joinOverTableScans);

            // join over TopN
            assertThat(query(session,
                    "SELECT * FROM (SELECT nationkey__c FROM nation__c ORDER BY regionkey__c LIMIT 5) n " +
                            "JOIN region__c r ON n.nationkey__c = r.regionkey__c"))
                    .isNotFullyPushedDown(joinOverTableScans);

            // join over join
            assertThat(query(session, "SELECT * FROM nation__c n, region__c r, customer__c c WHERE n.regionkey__c = r.regionkey__c AND r.regionkey__c = c.custkey__c"))
                    .isFullyPushedDown();
        }
    }

    private void assertConditionallyPushedDown(
            Session session,
            @Language("SQL") String query,
            boolean condition,
            PlanMatchPattern otherwiseExpected)
    {
        QueryAssertions.QueryAssert queryAssert = assertThat(query(session, query));
        if (condition) {
            queryAssert.isFullyPushedDown();
        }
        else {
            queryAssert.isNotFullyPushedDown(otherwiseExpected);
        }
    }

    private boolean expectJoinPushdown(String operator)
    {
        if ("IS NOT DISTINCT FROM".equals(operator)) {
            // TODO (https://github.com/trinodb/trino/issues/6967) support join pushdown for IS NOT DISTINCT FROM
            return false;
        }
        switch (toJoinConditionOperator(operator)) {
            case EQUAL:
            case NOT_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return true;
            case IS_DISTINCT_FROM:
                return hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM);
        }
        throw new AssertionError(); // unreachable
    }

    private boolean expectVarcharJoinPushdown(String operator)
    {
        if ("IS NOT DISTINCT FROM".equals(operator)) {
            // TODO (https://github.com/trinodb/trino/issues/6967) support join pushdown for IS NOT DISTINCT FROM
            return false;
        }
        switch (toJoinConditionOperator(operator)) {
            case EQUAL:
            case NOT_EQUAL:
                return hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY);
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY);
            case IS_DISTINCT_FROM:
                return hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM) && hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY);
        }
        throw new AssertionError(); // unreachable
    }

    private JoinCondition.Operator toJoinConditionOperator(String operator)
    {
        return Stream.of(JoinCondition.Operator.values())
                .filter(joinOperator -> joinOperator.getValue().equals(operator))
                .collect(toOptional())
                .orElseThrow(() -> new IllegalArgumentException("Not found: " + operator));
    }

    protected Session joinPushdownEnabled(Session session)
    {
        // If join pushdown gets enabled by default, tests should use default session
        verify(!new JdbcMetadataConfig().isJoinPushdownEnabled());
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_enabled", "true")
                .build();
    }
}
