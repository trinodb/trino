/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake;

import io.trino.Session;
import io.trino.execution.QueryManager;
import io.trino.sql.analyzer.FeaturesConfig;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestDistributedSnowflakeConnectorTest
        extends BaseSnowflakeConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return distributedBuilder()
                .withServer(server)
                .withAdditionalProperties(impersonationDisabled())
                .withConnectionPooling()
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                // TOPN is retained due to parallelism
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    public void testSimpleSelect()
    {
        assertQuery("SELECT regionkey, name FROM region ORDER BY regionkey", "VALUES (0, 'AFRICA'), (1, 'AMERICA'), (2, 'ASIA'), (3, 'EUROPE'), (4, 'MIDDLE EAST')");
    }

    @Test
    public void testLargeTableScan()
    {
        // Use "CREATE TABLE TEST_DB.TEST_SCHEMA_2.sf10_lineitem AS SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.lineitem" in Snowflake UI to create test table
        assertQuery("SELECT " +
                        "count(l_orderkey + 1), " +
                        "count(l_partkey + 1), " +
                        "count(l_suppkey + 1), " +
                        "count(l_linenumber + 1), " +
                        "count(l_quantity + 1), " +
                        "count(l_extendedprice + 1), " +
                        "count(l_discount + 1), " +
                        "count(l_tax + 1), " +
                        "count(l_returnflag || 'F'), " +
                        "count(l_linestatus || 'F'), " +
                        "count(l_shipdate + interval '1' day), " +
                        "count(l_commitdate + interval '1' day), " +
                        "count(l_receiptdate + interval '1' day), " +
                        "count(l_shipinstruct || 'F'), " +
                        "count(l_shipmode || 'F'), " +
                        "count(l_comment || 'F') " +
                        "FROM sf10_lineitem",
                "VALUES (59986052, 59986052, 59986052, 59986052, 59986052, 59986052, 59986052, 59986052, " +
                        "59986052, 59986052, 59986052, 59986052, 59986052, 59986052, 59986052, 59986052)");
    }

    @Test
    public void testDynamicFilterIsApplied()
    {
        String sql = "SELECT l.partkey FROM lineitem l JOIN nation n ON n.regionkey = l.orderkey AND n.name < 'B' ";

        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> dynamicFilter = queryRunner.executeWithQueryId(fixedBroadcastJoinDistribution(true), sql);
        ResultWithQueryId<MaterializedResult> noDynamicFilter = queryRunner.executeWithQueryId(fixedBroadcastJoinDistribution(false), sql);
        assertEquals(dynamicFilter.getResult().getOnlyColumnAsSet(), noDynamicFilter.getResult().getOnlyColumnAsSet());

        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        long dynamicFilterProcessedBytes = queryManager.getFullQueryInfo(dynamicFilter.getQueryId()).getQueryStats().getProcessedInputDataSize().toBytes();
        long noDynamicFilterProcessedBytes = queryManager.getFullQueryInfo(noDynamicFilter.getQueryId()).getQueryStats().getProcessedInputDataSize().toBytes();
        assertThat(dynamicFilterProcessedBytes).as("dynamicFilterProcessedBytes")
                .isLessThan(noDynamicFilterProcessedBytes);
    }

    @Test
    @Override
    public void testNumericAggregationPushdown()
    {
        // TODO https://starburstdata.atlassian.net/browse/SEP-4739
        assertThatThrownBy(super::testNumericAggregationPushdown)
                .hasMessageContaining("Error encountered when unloading FIXED data to PARQUET: SFLogicalType: FIXED, SFPhysicalType: SB8, length: 4, precision: 9, scale: 3");
    }

    @Override
    @Test
    public void testDropNonEmptySchema()
    {
        assertThatThrownBy(super::testDropNonEmptySchema)
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith("Expected query to fail: DROP SCHEMA ");
        throw new SkipException("DROP SCHEMA erases all tables in Snowflake connector"); // TODO (https://github.com/trinodb/trino/issues/8634)
    }

    private Session fixedBroadcastJoinDistribution(boolean dynamicFilteringEnabled)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.BROADCAST.name())
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, Boolean.toString(dynamicFilteringEnabled))
                .build();
    }
}
