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
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;
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

    private Session fixedBroadcastJoinDistribution(boolean dynamicFilteringEnabled)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.BROADCAST.name())
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, Boolean.toString(dynamicFilteringEnabled))
                .build();
    }
}
