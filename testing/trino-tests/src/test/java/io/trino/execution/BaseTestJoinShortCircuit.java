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
package io.trino.execution;

import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.server.DynamicFilterService.DynamicFiltersStats;
import io.trino.spi.predicate.Domain;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.EXECUTION_POLICY;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.SystemSessionProperties.JOIN_SHORT_CIRCUIT_ENABLED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs joins with a side that has no splits, against the {@code slow} catalog of {@link SplitsOverridingTpchPlugin},
 * whose source stage creates tasks one after another, so that the other side is still being read when the join
 * could stop reading it.
 */
public abstract class BaseTestJoinShortCircuit
        extends AbstractTestQueryFramework
{
    private static final long PARTSUPP_ROWS = 800_000;
    private static final String PROBE_WITHOUT_SPLITS = "SELECT count(*) FROM no_splits.tiny.part p JOIN slow.sf1.partsupp ps ON ps.partkey = p.partkey";

    protected abstract DistributedQueryRunner.Builder<?> createQueryRunnerBuilder(Session defaultSession);

    protected abstract boolean isDynamicFilteringEnabled();

    /**
     * Whether a scan stops once the join reading it stops, instead of every stage reading its whole input
     */
    protected abstract boolean isScanStoppedWithJoin();

    @Override
    protected final QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, Boolean.toString(isDynamicFilteringEnabled()))
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty(EXECUTION_POLICY, "all-at-once")
                .build();
        QueryRunner queryRunner = createQueryRunnerBuilder(session)
                .setWorkerCount(2)
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch", Map.of("tpch.splits-per-node", "2"));
            SplitsOverridingTpchPlugin.createCatalogs(queryRunner);
        }
        catch (Throwable e) {
            queryRunner.close();
            throw e;
        }
        return queryRunner;
    }

    @Test
    @Timeout(120)
    void testProbeWithoutSplitsOverPartitionedExchange()
    {
        MaterializedResultWithPlan result = execute(withJoinDistribution("PARTITIONED"), PROBE_WITHOUT_SPLITS);
        assertScannedRows(result, PARTSUPP_ROWS);
        DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(result.queryId());
        if (!isDynamicFilteringEnabled()) {
            assertThat(dynamicFiltersStats.getTotalDynamicFilters()).isZero();
            return;
        }
        assertThat(dynamicFiltersStats.getTotalDynamicFilters()).isEqualTo(1);
        assertThat(dynamicFiltersStats.getDynamicFiltersCompleted()).isEqualTo(1);
        String domain = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats()).getSimplifiedDomain();
        if (isScanStoppedWithJoin()) {
            assertThat(domain).isEqualTo(Domain.none(BIGINT).toString());
        }
        else {
            assertThat(domain).matches("\\[ SortedRangeSet\\[type=bigint, ranges=\\d+, \\{\\[1,.*,200000]}] ]");
        }
    }

    @Test
    @Timeout(120)
    void testProbeWithoutSplitsOverBroadcastExchange()
    {
        assertScannedRows(execute(withJoinDistribution("BROADCAST"), PROBE_WITHOUT_SPLITS), PARTSUPP_ROWS);
    }

    @Test
    @Timeout(120)
    void testProbeWithoutSplitsForCrossJoin()
    {
        assertScannedRows(
                execute(getSession(), "SELECT count(*) FROM no_splits.tiny.part p CROSS JOIN slow.sf1.partsupp ps WHERE ps.partkey < p.partkey"),
                PARTSUPP_ROWS);
    }

    @Test
    @Timeout(120)
    void testBuildWithoutSplits()
    {
        MaterializedResultWithPlan result = execute(withJoinDistribution("PARTITIONED"), "SELECT count(*) FROM slow.sf1.partsupp ps JOIN no_splits.tiny.part p ON ps.partkey = p.partkey");
        if (isScanStoppedWithJoin() || !isDynamicFilteringEnabled()) {
            assertScannedRows(result, PARTSUPP_ROWS);
        }
    }

    @Test
    @Timeout(120)
    void testProbeWithoutSplitsWithoutShortCircuit()
    {
        Session session = Session.builder(withJoinDistribution("BROADCAST"))
                .setSystemProperty(JOIN_SHORT_CIRCUIT_ENABLED, "false")
                .build();
        MaterializedResultWithPlan result = execute(session, PROBE_WITHOUT_SPLITS);
        assertThat(getScannedRows(result)).isEqualTo(PARTSUPP_ROWS);
    }

    private Session withJoinDistribution(String joinDistributionType)
    {
        return Session.builder(getSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, joinDistributionType)
                .build();
    }

    private MaterializedResultWithPlan execute(Session session, String sql)
    {
        MaterializedResultWithPlan result = getDistributedQueryRunner().executeWithPlan(session, sql);
        assertThat(result.result().getOnlyValue()).isEqualTo(0L);
        return result;
    }

    private void assertScannedRows(MaterializedResultWithPlan result, long otherSideRows)
    {
        if (isScanStoppedWithJoin()) {
            assertThat(getScannedRows(result)).isLessThan(otherSideRows);
        }
        else {
            assertThat(getScannedRows(result)).isEqualTo(otherSideRows);
        }
    }

    private long getScannedRows(MaterializedResultWithPlan result)
    {
        return getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(result.queryId()).getQueryStats().getPhysicalInputPositions();
    }
}
