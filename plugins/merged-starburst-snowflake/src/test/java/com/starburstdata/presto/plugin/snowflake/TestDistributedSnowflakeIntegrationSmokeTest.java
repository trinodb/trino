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

import io.prestosql.Session;
import io.prestosql.execution.QueryManager;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.ResultWithQueryId;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.distributedBuilder;
import static com.starburstdata.presto.plugin.snowflake.SnowflakeQueryRunner.impersonationDisabled;
import static io.prestosql.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.prestosql.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.prestosql.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static java.lang.Boolean.TRUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestDistributedSnowflakeIntegrationSmokeTest
        extends BaseSnowflakeIntegrationSmokeTest
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
        if (TRUE) {
            // TODO enable the test after https://github.com/prestosql/presto/pull/3581
            throw new SkipException("TODO enable the test");
        }

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
