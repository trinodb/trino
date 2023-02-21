/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.synapse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JoinOperator;
import io.trino.plugin.sqlserver.BaseSqlServerConnectorTest;
import io.trino.plugin.sqlserver.DataCompression;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.LimitNode;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.TopNNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.testing.DataProviders;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import io.trino.testng.services.ManageTestResources;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.MoreCollectors.toOptional;
import static com.starburstdata.trino.plugins.synapse.SynapseQueryRunner.createSynapseQueryRunner;
import static io.trino.plugin.jdbc.JdbcWriteSessionProperties.NON_TRANSACTIONAL_INSERT;
import static io.trino.plugin.jdbc.JoinOperator.FULL_JOIN;
import static io.trino.plugin.sqlserver.SqlServerSessionProperties.BULK_COPY_FOR_WRITE;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.trueFalse;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_AGGREGATION_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_CREATE_TABLE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_DELETE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_LIMIT_PUSHDOWN;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN_WITH_LIKE;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY;
import static io.trino.testing.TestingConnectorBehavior.SUPPORTS_TOPN_PUSHDOWN;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;

public class TestSynapseConnectorTest
        extends BaseSqlServerConnectorTest
{
    public static final String CATALOG = "sqlserver";
    @ManageTestResources.Suppress(because = "Mock to remote server")
    private SynapseServer synapseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        synapseServer = new SynapseServer();
        return createSynapseQueryRunner(
                Map.of(),
                synapseServer,
                CATALOG,
                // Synapse tests are slow. Cache metadata to speed them up. Synapse without caching is exercised by TestSynapseConnectorSmokeTest.
                Map.of("metadata.cache-ttl", "60m"),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            // Overriden because Synapse disables connector expression pushdown due to correctness issues with varchar pushdown because of default case-insensitive collation
            case SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected SqlExecutor onRemoteDatabase()
    {
        return synapseServer::execute;
    }

    @Override
    protected TestTable createTableWithDefaultColumns()
    {
        return new TestTable(
                onRemoteDatabase(),
                "table",
                "(col_required BIGINT NOT NULL," +
                        "col_nullable BIGINT," +
                        "col_default BIGINT DEFAULT 43," +
                        "col_nonnull_default BIGINT NOT NULL DEFAULT 42," +
                        "col_required2 BIGINT NOT NULL)");
    }

    @Override
    protected TestTable createTableWithUnsupportedColumn()
    {
        throw new SkipException("All Synapse types are mapped either by Trino or the SQL Server JDBC");
    }

    @Test
    @Override // default test execution too long due to wildcards in LIKE clause
    public void testSelectInformationSchemaColumns()
    {
        String schema = getSession().getSchema().get();
        assertThat(query("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'region'"))
                .skippingTypesCheck()
                .matches("VALUES 'regionkey', 'name', 'comment'");
        assertThat(query("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '_egion'"))
                .skippingTypesCheck()
                .matches("VALUES 'regionkey', 'name', 'comment'");
        assertThat(query("SELECT column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%egio%'"))
                .skippingTypesCheck()
                .matches("VALUES 'regionkey', 'name', 'comment'");
    }

    @Test
    @Override // Needs an override because the SQL Server override is different from the base version of the test
    public void testColumnComment()
    {
        throw new SkipException("Synapse does not support column comments");
    }

    @Test
    public void testDecimalPredicatePushdown()
            throws Exception
    {
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_decimal_pushdown", "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            onRemoteDatabase().execute(format("INSERT INTO %s VALUES (123.321, 123456789.987654321)", table.getName()));

            assertThat(query(String.format("SELECT * FROM %s WHERE short_decimal <= 124", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE short_decimal <= 124", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE long_decimal <= 123456790", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE short_decimal <= 123.321", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE long_decimal <= 123456789.987654321", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE short_decimal = 123.321", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
            assertThat(query(String.format("SELECT * FROM %s WHERE long_decimal = 123456789.987654321", table.getName())))
                    .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                    .isFullyPushedDown();
        }
    }

    @Test
    @Override // synapse doesn't support data_compression, so reverse SQL Server's override
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
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

    @Test(dataProvider = "dataCompression")
    @Override
    public void testCreateWithDataCompression(DataCompression compression)
    {
        throw new SkipException("data_compression not supported in Synapse");
    }

    @Test
    @Override
    public void testShowCreateForPartitionedTablesWithDataCompression()
    {
        throw new SkipException("CREATE PARTITION FUNCTION and data_compression not supported in Synapse");
    }

    @Test
    @Override
    public void testShowCreateForIndexedAndCompressedTable()
    {
        throw new SkipException("data_compression not supported in Synapse");
    }

    @Test
    @Override
    public void testShowCreateForUniqueConstraintCompressedTable()
    {
        throw new SkipException("data_compression not supported in Synapse");
    }

    @Override
    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        String typeName = dataMappingTestSetup.getTrinoTypeName();
        if (typeName.equals("timestamp(3) with time zone")) {
            return Optional.of(dataMappingTestSetup.asUnsupported());
        }

        return super.filterDataMappingSmokeTestData(dataMappingTestSetup);
    }

    @Override
    protected Session joinPushdownEnabled(Session session)
    {
        return Session.builder(super.joinPushdownEnabled(session))
                // strategy is AUTOMATIC by default and would not work for certain test cases (even if statistics are collected)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "join_pushdown_strategy", "EAGER")
                .build();
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testCreateTableAsSelectWriteBulkiness(boolean bulkCopyForWrite)
    {
        String table = "bulk_copy_ctas_" + randomNameSuffix();
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE, Boolean.toString(bulkCopyForWrite))
                .build();

        // loading data takes too long for this test, this test does not compare performance, just checks if the path passes, therefore LIMIT 1 is applied
        assertQuerySucceeds(session, format("CREATE TABLE %s as SELECT * FROM tpch.tiny.customer LIMIT 1", table));

        // check that there are no locks remained on the target table after bulk copy
        assertQuery("SELECT count(*) FROM " + table, "VALUES 1");
        assertUpdate(format("INSERT INTO %s SELECT * FROM tpch.tiny.customer LIMIT 1", table), 1);
        assertQuery("SELECT count(*) FROM " + table, "VALUES 2");

        assertUpdate("DROP TABLE " + table);
    }

    @Test
    public void testSpecialCharacterColumnNameFailToRename()
    {
        String table = "special_column_name_" + randomNameSuffix();
        String specialCharacterColumnName = "\"" + "[tricky]" + "\"";
        String normalColumnName = "normal";
        Session session = Session.builder(getSession()).build();
        assertQuerySucceeds(session, format("CREATE TABLE %s (%s bigint)", table, specialCharacterColumnName));

        // check that we are not able to rename column with special character name back,
        // our test should fail after synapse will fix this issue, we will be able to add support for such cases
        this.assertQueryFails(format("ALTER TABLE %s RENAME COLUMN %s TO %s", table, specialCharacterColumnName, normalColumnName), "\\QEither the parameter @objname is ambiguous or the claimed @objtype (COLUMN) is wrong.\\E");
    }

    @Test(dataProvider = "doubleTrueFalse")
    public void testInsertWriteBulkiness(boolean nonTransactionalInsert, boolean bulkCopyForWrite)
    {
        String table = "bulk_copy_insert_" + randomNameSuffix();
        assertQuerySucceeds(format("CREATE TABLE %s as SELECT * FROM tpch.tiny.customer WHERE 0 = 1", table));
        Session session = Session.builder(getSession())
                .setCatalogSessionProperty(CATALOG, NON_TRANSACTIONAL_INSERT, Boolean.toString(nonTransactionalInsert))
                .setCatalogSessionProperty(CATALOG, BULK_COPY_FOR_WRITE, Boolean.toString(bulkCopyForWrite))
                .build();

        // loading data takes too long for this test, this test does not compare performance, just checks if the path passes, therefore LIMIT 1 is applied
        assertQuerySucceeds(session, format("INSERT INTO %s SELECT * FROM tpch.tiny.customer LIMIT 1", table));

        // check that there are no locks remained on the target table after bulk copy
        assertQuery("SELECT count(*) FROM " + table, "VALUES 1");
        assertUpdate(format("INSERT INTO %s SELECT * FROM tpch.tiny.customer LIMIT 1", table), 1);
        assertQuery("SELECT count(*) FROM " + table, "VALUES 2");

        assertUpdate("DROP TABLE " + table);
    }

    @Override
    public void testDelete()
    {
        // TODO: Remove override once superclass uses smaller tables to test (because INSERTs to Synapse are slow)
        skipTestUnless(hasBehavior(SUPPORTS_DELETE) && hasBehavior(SUPPORTS_CREATE_TABLE));

        String tableName = "test_delete_" + randomNameSuffix();

        // delete successive parts of the table
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", "SELECT count(*) FROM nation");

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey <= 5", "SELECT count(*) FROM nation WHERE nationkey <= 5");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE nationkey > 5");

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey <= 10", "SELECT count(*) FROM nation WHERE nationkey > 5 AND nationkey <= 10");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE nationkey > 10");

        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey <= 20", "SELECT count(*) FROM nation WHERE nationkey > 10 AND nationkey <= 20");
        assertQuery("SELECT * FROM " + tableName, "SELECT * FROM nation WHERE nationkey > 20");

        assertUpdate("DROP TABLE " + tableName);

        // delete without matching any rows
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", "SELECT count(*) FROM nation");
        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey < 0", 0);
        assertUpdate("DROP TABLE " + tableName);

        // delete with a predicate that optimizes to false
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", "SELECT count(*) FROM nation");
        assertUpdate("DELETE FROM " + tableName + " WHERE nationkey > 5 AND nationkey < 4", 0);
        assertUpdate("DROP TABLE " + tableName);

        // test EXPLAIN ANALYZE with CTAS
        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE " + tableName + " AS SELECT nationkey FROM nation");
        assertQuery("SELECT * from " + tableName, "SELECT nationkey FROM nation");
        // check that INSERT works also
        assertExplainAnalyze("EXPLAIN ANALYZE INSERT INTO " + tableName + " SELECT regionkey FROM nation");
        assertQuery("SELECT * from " + tableName, "SELECT nationkey FROM nation UNION ALL SELECT regionkey FROM nation");
        // check DELETE works with EXPLAIN ANALYZE
        assertExplainAnalyze("EXPLAIN ANALYZE DELETE FROM " + tableName + " WHERE TRUE");
        assertQuery("SELECT COUNT(*) from " + tableName, "SELECT 0");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Override
    @Test
    public void testAggregationPushdown()
    {
        if (!hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN)) {
            assertThat(query("SELECT count(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
            return;
        }
        // TODO refactor BaseJdbcConnectorTest.testAggregationPushdown to be executed on copy of tpch table with case sensitive collation
        String caseSensitiveNation = "cs_nation" + randomNameSuffix();
        try {
            createTableAdjustCollation("nation",
                    caseSensitiveNation,
                    "name",
                    "NVARCHAR(25)",
                    "Latin1_General_CS_AS");

            // TODO support aggregation pushdown with GROUPING SETS
            // TODO support aggregation over expressions

            // count()
            assertThat(query("SELECT count(*) FROM nation")).isFullyPushedDown();
            assertThat(query("SELECT count(nationkey) FROM nation")).isFullyPushedDown();
            assertThat(query("SELECT count(1) FROM nation")).isFullyPushedDown();
            assertThat(query("SELECT count() FROM nation")).isFullyPushedDown();
            assertThat(query("SELECT regionkey, count(1) FROM nation GROUP BY regionkey")).isFullyPushedDown();
            try (TestTable emptyTable = createAggregationTestTable(getSession().getSchema().orElseThrow() + ".empty_table", ImmutableList.of())) {
                assertThat(query("SELECT count(*) FROM " + emptyTable.getName())).isFullyPushedDown();
                assertThat(query("SELECT count(a_bigint) FROM " + emptyTable.getName())).isFullyPushedDown();
                assertThat(query("SELECT count(1) FROM " + emptyTable.getName())).isFullyPushedDown();
                assertThat(query("SELECT count() FROM " + emptyTable.getName())).isFullyPushedDown();
                assertThat(query("SELECT a_bigint, count(1) FROM " + emptyTable.getName() + " GROUP BY a_bigint")).isFullyPushedDown();
            }

            // GROUP BY
            assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
            assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
            assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
            assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
            try (TestTable emptyTable = createAggregationTestTable(getSession().getSchema().orElseThrow() + ".empty_table", ImmutableList.of())) {
                assertThat(query("SELECT t_double, min(a_bigint) FROM " + emptyTable.getName() + " GROUP BY t_double")).isFullyPushedDown();
                assertThat(query("SELECT t_double, max(a_bigint) FROM " + emptyTable.getName() + " GROUP BY t_double")).isFullyPushedDown();
                assertThat(query("SELECT t_double, sum(a_bigint) FROM " + emptyTable.getName() + " GROUP BY t_double")).isFullyPushedDown();
                assertThat(query("SELECT t_double, avg(a_bigint) FROM " + emptyTable.getName() + " GROUP BY t_double")).isFullyPushedDown();
            }

            // GROUP BY and WHERE on bigint column
            // GROUP BY and WHERE on aggregation key
            assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 GROUP BY regionkey")).isFullyPushedDown();

            // GROUP BY and WHERE on varchar column
            // GROUP BY and WHERE on "other" (not aggregation key, not aggregation input)
            assertConditionallyPushedDown(
                    getSession(),
                    "SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 AND name > 'AAA' GROUP BY regionkey",
                    hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY),
                    node(FilterNode.class, node(TableScanNode.class)));
            // GROUP BY above WHERE and LIMIT
            assertConditionallyPushedDown(
                    getSession(),
                    "SELECT regionkey, sum(nationkey) FROM (SELECT * FROM nation WHERE regionkey < 2 LIMIT 11) GROUP BY regionkey",
                    hasBehavior(SUPPORTS_LIMIT_PUSHDOWN),
                    node(LimitNode.class, anyTree(node(TableScanNode.class))));
            // GROUP BY above TopN
            assertConditionallyPushedDown(
                    getSession(),
                    "SELECT custkey, sum(totalprice) FROM (SELECT custkey, totalprice FROM orders ORDER BY orderdate ASC, totalprice ASC LIMIT 10) GROUP BY custkey",
                    hasBehavior(SUPPORTS_TOPN_PUSHDOWN),
                    node(TopNNode.class, anyTree(node(TableScanNode.class))));
            // GROUP BY with JOIN
            assertConditionallyPushedDown(
                    joinPushdownEnabled(getSession()),
                    "SELECT n.regionkey, sum(c.acctbal) acctbals FROM nation n LEFT JOIN customer c USING (nationkey) GROUP BY 1",
                    hasBehavior(SUPPORTS_JOIN_PUSHDOWN),
                    node(JoinNode.class, anyTree(node(TableScanNode.class)), anyTree(node(TableScanNode.class))));
            // GROUP BY with WHERE on neither grouping nor aggregation column
            assertConditionallyPushedDown(
                    getSession(),
                    format("SELECT nationkey, min(regionkey) FROM %s WHERE name = 'ARGENTINA' GROUP BY nationkey", caseSensitiveNation),
                    hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY),
                    node(FilterNode.class, node(TableScanNode.class)));
            // GROUP BY with WHERE complex predicate
            assertConditionallyPushedDown(
                    getSession(),
                    "SELECT regionkey, sum(nationkey) FROM nation WHERE name LIKE '%N%' GROUP BY regionkey",
                    hasBehavior(SUPPORTS_PREDICATE_EXPRESSION_PUSHDOWN_WITH_LIKE),
                    node(FilterNode.class, node(TableScanNode.class)));
            // aggregation on varchar column
            assertThat(query("SELECT count(name) FROM nation")).isFullyPushedDown();
            // aggregation on varchar column with GROUPING
            assertThat(query("SELECT nationkey, count(name) FROM nation GROUP BY nationkey")).isFullyPushedDown();
            // aggregation on varchar column with WHERE
            assertConditionallyPushedDown(
                    getSession(),
                    format("SELECT count(name) FROM %s WHERE name = 'ARGENTINA'", caseSensitiveNation),
                    hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY),
                    node(FilterNode.class, node(TableScanNode.class)));

            // pruned away aggregation
            assertThat(query("SELECT -13 FROM (SELECT count(*) FROM nation)"))
                    .matches("VALUES -13")
                    .hasPlan(node(OutputNode.class, node(ValuesNode.class)));
            // aggregation over aggregation
            assertThat(query("SELECT count(*) FROM (SELECT count(*) FROM nation)"))
                    .matches("VALUES BIGINT '1'")
                    .hasPlan(node(OutputNode.class, node(ValuesNode.class)));
            assertThat(query("SELECT count(*) FROM (SELECT count(*) FROM nation GROUP BY regionkey)"))
                    .matches("VALUES BIGINT '5'")
                    .isFullyPushedDown();

            // aggregation with UNION ALL and aggregation
            assertThat(query("SELECT count(*) FROM (SELECT name FROM nation UNION ALL SELECT name FROM region)"))
                    .matches("VALUES BIGINT '30'")
                    // TODO (https://github.com/trinodb/trino/issues/12547): support count(*) over UNION ALL pushdown
                    .isNotFullyPushedDown(
                            node(ExchangeNode.class,
                                    node(AggregationNode.class, node(TableScanNode.class)),
                                    node(AggregationNode.class, node(TableScanNode.class))));

            // aggregation with UNION ALL and aggregation
            assertThat(query("SELECT count(*) FROM (SELECT count(*) FROM nation UNION ALL SELECT count(*) FROM region)"))
                    .matches("VALUES BIGINT '2'")
                    .hasPlan(
                            // Note: engine could fold this to single ValuesNode
                            node(OutputNode.class,
                                    node(AggregationNode.class,
                                            node(ExchangeNode.class,
                                                    node(ExchangeNode.class,
                                                            node(AggregationNode.class, node(ValuesNode.class)),
                                                            node(AggregationNode.class, node(ValuesNode.class)))))));
        }
        finally {
            dropTable(caseSensitiveNation);
        }
    }

    @SuppressWarnings({"deprecation", "DeprecatedApi"})
    @Override
    @Test(dataProvider = "joinOperators")
    public void testJoinPushdown(JoinOperator joinOperator)
    {
        // TODO refactor BaseJdbcConnectorTest.testJoinPushdown to be executed on copy of tpch table with case sensitive collation
        String caseSensitiveNation = "cs_nation" + randomNameSuffix();
        String caseSensitiveCustomer = "cs_customer" + randomNameSuffix();
        try {
            createTableAdjustCollation("nation",
                    caseSensitiveNation,
                    "name",
                    "NVARCHAR(25)",
                    "Latin1_General_CS_AS");
            createTableAdjustCollation("customer",
                    caseSensitiveCustomer,
                    "address",
                    "NVARCHAR(40)",
                    "Latin1_General_CS_AS");
            Session session = joinPushdownEnabled(getSession());

            if (!hasBehavior(SUPPORTS_JOIN_PUSHDOWN)) {
                assertThat(query(session, "SELECT r.name, n.name FROM nation n JOIN region r ON n.regionkey = r.regionkey"))
                        .joinIsNotFullyPushedDown();
                return;
            }

            if (joinOperator == FULL_JOIN && !hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_FULL_JOIN)) {
                // Covered by verifySupportsJoinPushdownWithFullJoinDeclaration
                return;
            }

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
                    "AS SELECT nationkey, lower(name) name, regionkey FROM nation")) {
                adjustCollation(nationLowercaseTable.getName(), "name", "NVARCHAR(25)", "Latin1_General_CS_AS");
                // basic case
                assertThat(query(session, format("SELECT r.name, n.name FROM nation n %s region r ON n.regionkey = r.regionkey", joinOperator))).isFullyPushedDown();

                // join over different columns
                assertThat(query(session, format("SELECT r.name, n.name FROM nation n %s region r ON n.nationkey = r.regionkey", joinOperator))).isFullyPushedDown();

                // pushdown when using USING
                assertThat(query(session, format("SELECT r.name, n.name FROM nation n %s region r USING(regionkey)", joinOperator))).isFullyPushedDown();

                // varchar equality predicate
                assertJoinConditionallyPushedDown(
                        session,
                        format("SELECT n.name, n2.regionkey FROM %1$s n %2$s %1$s n2 ON n.name = n2.name", caseSensitiveNation, joinOperator),
                        hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY));
                assertJoinConditionallyPushedDown(
                        session,
                        format("SELECT n.name, nl.regionkey FROM %s n %s %s nl ON n.name = nl.name", caseSensitiveNation, joinOperator, nationLowercaseTable.getName()),
                        hasBehavior(SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY));

                // multiple bigint predicates
                assertThat(query(session, format("SELECT n.name, c.name FROM nation n %s customer c ON n.nationkey = c.nationkey and n.regionkey = c.custkey", joinOperator)))
                        .isFullyPushedDown();

                // inequality
                for (String operator : nonEqualities) {
                    // bigint inequality predicate
                    assertJoinConditionallyPushedDown(
                            withoutDynamicFiltering,
                            format("SELECT r.name, n.name FROM nation n %s region r ON n.regionkey %s r.regionkey", joinOperator, operator),
                            expectJoinPushdown(operator) && expectJoinPushdowOnInequalityOperator(joinOperator));

                    // varchar inequality predicate
                    assertJoinConditionallyPushedDown(
                            withoutDynamicFiltering,
                            format("SELECT n.name, nl.name FROM %s n %s %s nl ON n.name %s nl.name", caseSensitiveNation, joinOperator, nationLowercaseTable.getName(), operator),
                            expectVarcharJoinPushdown(operator) && expectJoinPushdowOnInequalityOperator(joinOperator));
                }

                // inequality along with an equality, which constitutes an equi-condition and allows filter to remain as part of the Join
                for (String operator : nonEqualities) {
                    assertJoinConditionallyPushedDown(
                            session,
                            format("SELECT n.name, c.name FROM nation n %s customer c ON n.nationkey = c.nationkey AND n.regionkey %s c.custkey", joinOperator, operator),
                            expectJoinPushdown(operator));
                }

                // varchar inequality along with an equality, which constitutes an equi-condition and allows filter to remain as part of the Join
                for (String operator : nonEqualities) {
                    assertJoinConditionallyPushedDown(
                            session,
                            format("SELECT n.name, nl.name FROM %s n %s %s nl ON n.regionkey = nl.regionkey AND n.name %s nl.name", caseSensitiveNation, joinOperator, nationLowercaseTable.getName(), operator),
                            expectVarcharJoinPushdown(operator));
                }

                // Join over a (double) predicate
                assertThat(query(session, format("" +
                        "SELECT c.name, n.name " +
                        "FROM (SELECT * FROM customer WHERE acctbal > 8000) c " +
                        "%s nation n ON c.custkey = n.nationkey", joinOperator)))
                        .isFullyPushedDown();

                // Join over a varchar equality predicate
                assertJoinConditionallyPushedDown(
                        session,
                        format("SELECT c.name, n.name FROM (SELECT * FROM %s WHERE address = 'TcGe5gaZNgVePxU5kRrvXBfkasDTea') c " +
                                "%s nation n ON c.custkey = n.nationkey", caseSensitiveCustomer, joinOperator),
                        hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_EQUALITY));

                // Join over a varchar inequality predicate
                assertJoinConditionallyPushedDown(
                        session,
                        format("SELECT c.name, n.name FROM (SELECT * FROM %s WHERE address < 'TcGe5gaZNgVePxU5kRrvXBfkasDTea') c " +
                                "%s nation n ON c.custkey = n.nationkey", caseSensitiveCustomer, joinOperator),
                        hasBehavior(SUPPORTS_PREDICATE_PUSHDOWN_WITH_VARCHAR_INEQUALITY));

                // join over aggregation
                assertJoinConditionallyPushedDown(
                        session,
                        format("SELECT * FROM (SELECT regionkey rk, count(nationkey) c FROM nation GROUP BY regionkey) n " +
                                "%s region r ON n.rk = r.regionkey", joinOperator),
                        hasBehavior(SUPPORTS_AGGREGATION_PUSHDOWN));

                // join over LIMIT
                assertJoinConditionallyPushedDown(
                        session,
                        format("SELECT * FROM (SELECT nationkey FROM nation LIMIT 30) n " +
                                "%s region r ON n.nationkey = r.regionkey", joinOperator),
                        hasBehavior(SUPPORTS_LIMIT_PUSHDOWN));

                // join over TopN
                assertJoinConditionallyPushedDown(
                        session,
                        format("SELECT * FROM (SELECT nationkey FROM nation ORDER BY regionkey LIMIT 5) n " +
                                "%s region r ON n.nationkey = r.regionkey", joinOperator),
                        hasBehavior(SUPPORTS_TOPN_PUSHDOWN));

                // join over join
                assertThat(query(session, "SELECT * FROM nation n, region r, customer c WHERE n.regionkey = r.regionkey AND r.regionkey = c.custkey"))
                        .isFullyPushedDown();
            }
        }
        finally {
            dropTable(caseSensitiveNation);
            dropTable(caseSensitiveCustomer);
        }
    }

    @SuppressWarnings({"deprecation", "DeprecatedApi"})
    private boolean expectVarcharJoinPushdown(String operator)
    {
        if ("IS NOT DISTINCT FROM" .equals(operator)) {
            return false;
        }
        else {
            switch (this.toJoinConditionOperator(operator)) {
                case EQUAL:
                case NOT_EQUAL:
                    return this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY);
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    return this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_INEQUALITY);
                case IS_DISTINCT_FROM:
                    return this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_DISTINCT_FROM) && this.hasBehavior(TestingConnectorBehavior.SUPPORTS_JOIN_PUSHDOWN_WITH_VARCHAR_EQUALITY);
                default:
                    throw new AssertionError();
            }
        }
    }

    @SuppressWarnings({"deprecation", "DeprecatedApi"})
    private JoinCondition.Operator toJoinConditionOperator(String operator)
    {
        return Stream.of(JoinCondition.Operator.values())
                .filter(joinOperator -> joinOperator.getValue().equals(operator))
                .collect(toOptional())
                .orElseThrow(() -> new IllegalArgumentException("Not found: " + operator));
    }

    @Override
    public void testPredicatePushdown()
    {
        // TODO refactor BaseSqlServerConnectorTest.testPredicatePushdown to be executed on copy of tpch table with case sensitive collation
        String caseSensitiveNation = "cs_nation" + randomNameSuffix();
        try {
            createTableAdjustCollation("nation",
                    caseSensitiveNation,
                    "name",
                    "NVARCHAR(25)",
                    "Latin1_General_CS_AS");
            // varchar inequality
            assertThat(query(format("SELECT regionkey, nationkey, name FROM %s WHERE name != 'ROMANIA' AND name != 'ALGERIA'", caseSensitiveNation)))
                    .isFullyPushedDown();

            // varchar equality
            assertThat(query(format("SELECT regionkey, nationkey, name FROM %s WHERE name = 'ROMANIA'", caseSensitiveNation)))
                    .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                    .isFullyPushedDown();

            // varchar range
            assertThat(query(format("SELECT regionkey, nationkey, name FROM %s WHERE name BETWEEN 'POLAND' AND 'RPA'", caseSensitiveNation)))
                    .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                    // We are not supporting range predicate pushdown for varchars
                    .isNotFullyPushedDown(FilterNode.class);

            // varchar NOT IN
            assertThat(query(format("SELECT regionkey, nationkey, name FROM %s WHERE name NOT IN ('POLAND', 'ROMANIA', 'VIETNAM')", caseSensitiveNation)))
                    .isFullyPushedDown();

            // varchar NOT IN with small compaction threshold
            assertThat(query(
                    Session.builder(getSession())
                            .setCatalogSessionProperty("sqlserver", "domain_compaction_threshold", "1")
                            .build(),
                    format("SELECT regionkey, nationkey, name FROM %s WHERE name NOT IN ('POLAND', 'ROMANIA', 'VIETNAM')", caseSensitiveNation)))
                    // no pushdown because it was converted to range predicate
                    .isNotFullyPushedDown(
                            node(
                                    FilterNode.class,
                                    // verify that no constraint is applied by the connector
                                    tableScan(
                                            tableHandle -> ((JdbcTableHandle) tableHandle).getConstraint().isAll(),
                                            TupleDomain.all(),
                                            ImmutableMap.of())));

            // varchar IN without domain compaction
            assertThat(query(format("SELECT regionkey, nationkey, name FROM %s WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')", caseSensitiveNation)))
                    .matches("VALUES " +
                            "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                            "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                    .isFullyPushedDown();

            // varchar IN with small compaction threshold
            assertThat(query(
                    Session.builder(getSession())
                            .setCatalogSessionProperty("sqlserver", "domain_compaction_threshold", "1")
                            .build(),
                    format("SELECT regionkey, nationkey, name FROM %s WHERE name IN ('POLAND', 'ROMANIA', 'VIETNAM')", caseSensitiveNation)))
                    .matches("VALUES " +
                            "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                            "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                    // no pushdown because it was converted to range predicate
                    .isNotFullyPushedDown(
                            node(
                                    FilterNode.class,
                                    // verify that no constraint is applied by the connector
                                    tableScan(
                                            tableHandle -> ((JdbcTableHandle) tableHandle).getConstraint().isAll(),
                                            TupleDomain.all(),
                                            ImmutableMap.of())));

            // varchar different case
            assertThat(query(format("SELECT regionkey, nationkey, name FROM %s WHERE name = 'romania'", caseSensitiveNation)))
                    .returnsEmptyResult()
                    .isFullyPushedDown();

            // bigint equality
            assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                    .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                    .isFullyPushedDown();

            // bigint equality with small compaction threshold
            assertThat(query(
                    Session.builder(getSession())
                            .setCatalogSessionProperty("sqlserver", "domain_compaction_threshold", "1")
                            .build(),
                    "SELECT regionkey, nationkey, name FROM nation WHERE nationkey IN (19, 21)"))
                    .matches("VALUES " +
                            "(BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25))), " +
                            "(BIGINT '2', BIGINT '21', CAST('VIETNAM' AS varchar(25)))")
                    .isNotFullyPushedDown(FilterNode.class);

            // bigint range, with decimal to bigint simplification
            assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey BETWEEN 18.5 AND 19.5"))
                    .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                    .isFullyPushedDown();

            // date equality
            assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                    .matches("VALUES BIGINT '1250', 34406, 38436, 57570")
                    .isFullyPushedDown();

            // predicate over aggregation key (likely to be optimized before being pushed down into the connector)
            assertThat(query("SELECT * FROM (SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey) WHERE regionkey = 3"))
                    .matches("VALUES (BIGINT '3', BIGINT '77')")
                    .isFullyPushedDown();

            // predicate over aggregation result
            assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey HAVING sum(nationkey) = 77"))
                    .matches("VALUES (BIGINT '3', BIGINT '77')")
                    .isFullyPushedDown();

            // decimals
            try (TestTable testTable = new TestTable(
                    onRemoteDatabase(),
                    "test_decimal_pushdown",
                    "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))",
                    List.of("123.321, 123456789.987654321"))) {
                assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal <= 124"))
                        .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                        .isFullyPushedDown();
                assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal <= 124"))
                        .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                        .isFullyPushedDown();
                assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal <= 123456790"))
                        .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                        .isFullyPushedDown();
                assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal <= 123.321"))
                        .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                        .isFullyPushedDown();
                assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal <= 123456789.987654321"))
                        .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                        .isFullyPushedDown();
                assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE short_decimal = 123.321"))
                        .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                        .isFullyPushedDown();
                assertThat(query("SELECT * FROM " + testTable.getName() + " WHERE long_decimal = 123456789.987654321"))
                        .matches("VALUES (CAST(123.321 AS decimal(9,3)), CAST(123456789.987654321 AS decimal(30, 10)))")
                        .isFullyPushedDown();

                // varchar predicate over join
                Session joinPushdownEnabled = joinPushdownEnabled(getSession());
                assertThat(query(joinPushdownEnabled, format("SELECT c.name, n.name FROM customer c JOIN %s n ON c.custkey = n.nationkey WHERE n.name = 'POLAND'", caseSensitiveNation)))
                        .isFullyPushedDown();

                // join on varchar columns is not pushed down
                assertThat(query(joinPushdownEnabled, format("SELECT n.name, n2.regionkey FROM %1$s n JOIN %1$s n2 ON n.name = n2.name", caseSensitiveNation)))
                        .isFullyPushedDown();
            }
        }
        finally {
            dropTable(caseSensitiveNation);
        }
    }

    private void createTableAdjustCollation(
            String sourceTable,
            String destinationTableName,
            String column,
            String columnType,
            String collation)
    {
        String createSql = format("CREATE TABLE %s WITH (DISTRIBUTION = ROUND_ROBIN) AS SELECT * FROM %s", destinationTableName, sourceTable);
        onRemoteDatabase().execute(createSql);
        adjustCollation(destinationTableName, column, columnType, collation);
    }

    private void adjustCollation(String tableName,
            String column,
            String columnType,
            String collation)
    {
        String adjustCollationSql = format("ALTER TABLE %s ALTER COLUMN %s %s COLLATE %s", tableName, column, columnType, collation);
        onRemoteDatabase().execute(adjustCollationSql);
    }

    private void dropTable(String tableName)
    {
        onRemoteDatabase().execute(format("DROP TABLE %s", tableName));
    }

    @Test
    @Override
    public void testDeleteWithVarcharInequalityPredicate()
    {
        // overriding this method, because default collation is case insensitive comparing to SqlServer
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_delete_varchar", "(col varchar(1) COLLATE Latin1_General_CS_AS)", ImmutableList.of("'a'", "'A'", "null"))) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE col != 'A'", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 'A', null");
        }
    }

    @Test
    @Override
    public void testDeleteWithVarcharEqualityPredicate()
    {
        // overriding this method, because default collation is case insensitive comparing to SqlServer
        try (TestTable table = new TestTable(onRemoteDatabase(), "test_delete_varchar", "(col varchar(1) COLLATE Latin1_General_CS_AS)", ImmutableList.of("'a'", "'A'", "null"))) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE col = 'A'", 1);
            assertQuery("SELECT * FROM " + table.getName(), "VALUES 'a', null");
        }
    }

    @Override
    public void testDeleteWithLike()
    {
        assertThatThrownBy(super::testDeleteWithLike)
                .hasStackTraceContaining("TrinoException: This connector does not support modifying table rows");
    }

    @Override
    @Test
    public void testReadMetadataWithRelationsConcurrentModifications()
    {
        // TODO Fix concurrent metadata modification test https://starburstdata.atlassian.net/browse/SEP-8789
        throw new SkipException("Test fails with a timeout sometimes and is flaky");
    }

    @Override
    public void testInsertRowConcurrently()
    {
        throw new SkipException("Synapse INSERTs are slow and the futures sometimes timeout in the test. TODO https://starburstdata.atlassian.net/browse/SEP-9214");
    }

    @Override
    protected String errorMessageForInsertIntoNotNullColumn(String columnName)
    {
        return format("(?s)Cannot insert the value NULL into column '%s'.*", columnName);
    }

    @Override
    protected void verifySchemaNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e).hasMessageMatching("Parse Error: Identifier '.*' exceeded the maximum length of 128.");
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable e)
    {
        assertThat(e)
                .hasMessageMatching("(Parse Error: Identifier '.*' exceeded the maximum length of 128.|Table name must be shorter than or equal to '128' characters but got '129')");
    }

    @DataProvider
    public static Object[][] doubleTrueFalse()
    {
        return cartesianProduct(trueFalse(), trueFalse());
    }

    @Override // Override because the JDBC prepares the query, but does not provide ResultSetMetadata
    public void testNativeQueryInsertStatementTableDoesNotExist()
    {
        assertFalse(getQueryRunner().tableExists(getSession(), "non_existent_table"));
        assertThatThrownBy(() -> query("SELECT * FROM TABLE(system.query(query => 'INSERT INTO non_existent_table VALUES (1)'))"))
                .hasMessageContaining("Query not supported: ResultSetMetaData not available for query: INSERT INTO non_existent_table VALUES (1)");
    }
}
