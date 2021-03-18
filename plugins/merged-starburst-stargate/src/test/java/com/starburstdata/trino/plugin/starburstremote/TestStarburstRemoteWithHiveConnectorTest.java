/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.starburstremote;

import io.trino.Session;
import io.trino.plugin.jdbc.BaseJdbcConnectorTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.MarkDistinctNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.SqlExecutor;
import io.trino.testing.sql.TestTable;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunner;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.createStarburstRemoteQueryRunnerWithHive;
import static com.starburstdata.trino.plugin.starburstremote.StarburstRemoteQueryRunner.starburstRemoteConnectionUrl;
import static io.trino.SystemSessionProperties.USE_MARK_DISTINCT;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarburstRemoteWithHiveConnectorTest
        extends BaseJdbcConnectorTest
{
    private DistributedQueryRunner remoteStarburst;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path tempDir = createTempDirectory("HiveCatalog");
        closeAfterClass(() -> deleteRecursively(tempDir, ALLOW_INSECURE));

        remoteStarburst = closeAfterClass(createStarburstRemoteQueryRunnerWithHive(
                tempDir,
                Map.of(),
                REQUIRED_TPCH_TABLES,
                Optional.empty()));
        return createStarburstRemoteQueryRunner(
                false,
                Map.of(),
                Map.of(
                        "connection-url", starburstRemoteConnectionUrl(remoteStarburst, getRemoteCatalogName()),
                        "allow-drop-table", "true"));
    }

    protected SqlExecutor onRemoteDatabase()
    {
        return remoteStarburst::execute;
    }

    protected String getRemoteCatalogName()
    {
        return "hive";
    }

    @Override
    @SuppressWarnings("DuplicateBranchesInSwitch")
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                // not yet supported in Remote connector
                return false;

            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_RENAME_TABLE:
                // Writes are not enabled
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
                // not yet supported in Remote connector
                return false;
            case SUPPORTS_COMMENT_ON_COLUMN:
                return true;

            case SUPPORTS_CREATE_VIEW:
                // TODO Add support in Remote connector (https://starburstdata.atlassian.net/browse/PRESTO-4795)
                return false;

            case SUPPORTS_INSERT:
            case SUPPORTS_DELETE:
                // Writes are not enabled
                return false;

            case SUPPORTS_ARRAY:
                // TODO Add support in Remote connector (https://starburstdata.atlassian.net/browse/PRESTO-4798)
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    public void testCommentColumn()
    {
        // TODO (https://starburstdata.atlassian.net/browse/PRESTO-4832) make sure this is tested
        assertThatThrownBy(super::testCommentColumn)
                .hasMessageStartingWith("This connector does not support creating tables");
        throw new SkipException("not supported");
    }

    @Test
    public void testReadFromRemoteHiveView()
    {
        onRemoteDatabase().execute("CREATE OR REPLACE VIEW hive.tiny.nation_count AS SELECT COUNT(*) cnt FROM tpch.tiny.nation");
        assertQuery("SELECT cnt FROM nation_count", "SELECT 25");
    }

    @Test
    public void testAggregationPushdown()
    {
        // TODO support aggregation pushdown with GROUPING SETS
        // TODO support aggregation over expressions

        // SELECT DISTINCT
        assertThat(query("SELECT DISTINCT regionkey FROM nation")).isFullyPushedDown();

        // count()
        assertThat(query("SELECT count(*) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(nationkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(1) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count() FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();

        // GROUP BY
        assertThat(query("SELECT regionkey, min(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, max(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, avg(nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        Session withMarkDistinct = Session.builder(getSession())
                .setSystemProperty(USE_MARK_DISTINCT, "true")
                .build();

        // distinct aggregation
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT comment) FROM nation")).isFullyPushedDown();
        // two distinct aggregations
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation"))
                .isNotFullyPushedDown(MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);
        // distinct aggregation and a non-distinct aggregation
        assertThat(query(withMarkDistinct, "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation"))
                .isNotFullyPushedDown(MarkDistinctNode.class, ExchangeNode.class, ExchangeNode.class, ProjectNode.class);

        Session withoutMarkDistinct = Session.builder(getSession())
                .setSystemProperty(USE_MARK_DISTINCT, "false")
                .build();

        // distinct aggregation
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey) FROM nation")).isFullyPushedDown();
        // distinct aggregation with GROUP BY
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();
        // distinct aggregation with varchar
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT comment) FROM nation")).isFullyPushedDown();
        // two distinct aggregations
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey), count(DISTINCT nationkey) FROM nation"))
                .isFullyPushedDown();
        // distinct aggregation and a non-distinct aggregation
        assertThat(query(withoutMarkDistinct, "SELECT count(DISTINCT regionkey), sum(nationkey) FROM nation"))
                .isFullyPushedDown();

        // GROUP BY and WHERE on bigint column
        // GROUP BY and WHERE on aggregation key
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 GROUP BY regionkey")).isFullyPushedDown();

        // GROUP BY and WHERE on varchar column
        // GROUP BY and WHERE on "other" (not aggregation key, not aggregation input)
        assertThat(query("SELECT regionkey, sum(nationkey) FROM nation WHERE regionkey < 4 AND name > 'AAA' GROUP BY regionkey")).isFullyPushedDown();

        // decimals
        try (TestTable testTable = new TestTable(
                onRemoteDatabase(),
                format("%s.%s.%s", getRemoteCatalogName(), getSession().getSchema().orElseThrow(), "test_aggregation_pushdown"),
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))",
                List.of(
                        "(100.000, 100000000.000000000)",
                        "(123.321, 123456789.987654321)"))) {
            String tableName = "p2p_" + testTable.getName().replace(getRemoteCatalogName(), "remote");
            assertThat(query("SELECT min(short_decimal), min(long_decimal) FROM " + tableName)).isFullyPushedDown();
            assertThat(query("SELECT max(short_decimal), max(long_decimal) FROM " + tableName)).isFullyPushedDown();
            assertThat(query("SELECT sum(short_decimal), sum(long_decimal) FROM " + tableName)).isFullyPushedDown();
            assertThat(query("SELECT avg(short_decimal), avg(long_decimal) FROM " + tableName)).isFullyPushedDown();
        }
    }

    @Test
    public void testAggregationPushdownWithPrestoSpecificFunctions()
    {
        assertThat(query("SELECT approx_distinct(regionkey) FROM nation")).isFullyPushedDown();
        assertThat(query("SELECT approx_distinct(name) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        assertThat(query("SELECT max_by(name, nationkey) FROM nation GROUP BY regionkey")).isFullyPushedDown();

        // checksum returns varbinary
        assertThat(query("SELECT checksum(nationkey) FROM nation")).isFullyPushedDown();

        // geometric_mean returns double
        assertThat(query("SELECT geometric_mean(nationkey) FROM nation")).isFullyPushedDown();
    }

    @Test
    public void testAggregationWithUnsupportedResultType()
    {
        // array_agg returns array, which is not supported
        assertThat(query("SELECT array_agg(regionkey) FROM nation WHERE nationkey = 3")).isNotFullyPushedDown(AggregationNode.class);

        // histogram returns map, which is not supported
        assertThat(query("SELECT histogram(regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // multimap_agg returns multimap, which is not supported
        assertThat(query("SELECT multimap_agg(nationkey, regionkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);

        // approx_set returns HyperLogLog, which is not supported
        assertThat(query("SELECT approx_set(nationkey) FROM nation")).isNotFullyPushedDown(AggregationNode.class);
    }

    @Test
    public void testLimitPushdown()
    {
        assertThat(query("SELECT name FROM nation LIMIT 30")).isFullyPushedDown(); // Use high limit for result determinism

        // with filter over numeric column
        assertThat(query("SELECT name FROM nation WHERE regionkey = 3 LIMIT 5")).isFullyPushedDown();

        // with filter over varchar column
        assertThat(query("SELECT name FROM nation WHERE name < 'EEE' LIMIT 5")).isFullyPushedDown();

        // with aggregation
        assertThat(query("SELECT max(regionkey) FROM nation LIMIT 5")).isFullyPushedDown(); // global aggregation, LIMIT removed
        assertThat(query("SELECT regionkey, max(name) FROM nation GROUP BY regionkey LIMIT 5")).isFullyPushedDown();
        assertThat(query("SELECT DISTINCT regionkey FROM nation LIMIT 5")).isFullyPushedDown();

        // with filter and aggregation
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE nationkey < 5 GROUP BY regionkey LIMIT 3")).isFullyPushedDown();
        assertThat(query("SELECT regionkey, count(*) FROM nation WHERE name < 'EGYPT' GROUP BY regionkey LIMIT 3")).isFullyPushedDown();
    }

    @Test
    public void testCreateView()
    {
        // The test ensures that we do not inherit CREATE VIEW capabilities from trino-base-jdbc.
        // While generic VIEW support in base JDBC is feasible, Starburst Remote connector would require special
        // considerations, see https://starburstdata.atlassian.net/browse/PRESTO-4795.
        assertThatThrownBy(() -> query("CREATE VIEW v AS SELECT 1 a"))
                .hasMessage("This connector does not support creating views");
    }

    @Test
    public void testPredicatePushdown()
    {
        // varchar equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'ROMANIA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar range
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name BETWEEN 'POLAND' AND 'RPA'"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // varchar different case
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE name = 'romania'"))
                .returnsEmptyResult()
                .isFullyPushedDown();

        // bigint equality
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey = 19"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // bigint range, with decimal to bigint simplification
        assertThat(query("SELECT regionkey, nationkey, name FROM nation WHERE nationkey BETWEEN 18.5 AND 19.5"))
                .matches("VALUES (BIGINT '3', BIGINT '19', CAST('ROMANIA' AS varchar(25)))")
                .isFullyPushedDown();

        // date equality
        assertThat(query("SELECT orderkey FROM orders WHERE orderdate = DATE '1992-09-29'"))
                .matches("VALUES BIGINT '1250', 34406, 38436, 57570")
                .isFullyPushedDown();
    }
}
